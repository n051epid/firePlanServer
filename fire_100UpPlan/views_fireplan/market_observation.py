import akshare as ak
import pandas as pd
import numpy as np
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from datetime import datetime, timedelta
from rest_framework.permissions import IsAuthenticated, AllowAny
from django.db import models
from fire_100UpPlan.models import MarketValuation, IndexData, MarginTradingData, IndustryValuation, BondIndexData, BondData, BigDataStrategyStockData
from django.db.models import Max
from django.db.models.functions import ExtractWeekDay
import logging
from django.db.models.functions import Extract
from django.conf import settings
from openai import OpenAI
from django.db.models.functions import Cast, Substr
from django.db.models import IntegerField, DateField, Min, Max, F, Q, Case, When, Value
from django.http import StreamingHttpResponse
import json
from django.http import JsonResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from fire_100UpPlan.utils import error_response

import threading
import os
import requests

# ========================================
# Proxy + AKTools 系统 (from fetcher.py)
# ========================================

_proxy_manager_instance = None
_proxy_manager_lock = threading.Lock()

class ProxyManager:
    """
    轻量级代理管理器
    - 支持 HTTP/HTTPS 代理
    - 支持认证代理 (user:pass@host:port)
    - 多代理轮询负载均衡
    - 自动故障转移
    """
    
    def __init__(self, proxy_urls):
        self._proxies = proxy_urls
        self._current_index = 0
        self._failed_proxies = set()
        self._stats = {p: {'success': 0, 'fail': 0, 'consecutive_fails': 0} for p in proxy_urls}
        self._lock = threading.Lock()
        self._direct_mode = False
        
        # 保存原始 requests 方法
        self._original_get = requests.get
        self._original_post = requests.post
        
        import logging
        logging.getLogger(__name__).info(f"ProxyManager: 初始化完成，代理数量: {len(proxy_urls)}")
    
    def get_proxy(self):
        """获取下一个可用代理"""
        with self._lock:
            if self._direct_mode or not self._proxies:
                return None
            
            available = [p for p in self._proxies if p not in self._failed_proxies]
            if not available:
                import logging
                logging.getLogger(__name__).warning("ProxyManager: 所有代理失效，回退到直连模式")
                self._direct_mode = True
                return None
            
            proxy = available[self._current_index % len(available)]
            self._current_index += 1
            proxy_https = proxy.replace('http://', 'https://')
            return {'http': proxy, 'https': proxy_https}
    
    def mark_success(self, proxy_url):
        """标记请求成功"""
        with self._lock:
            if proxy_url in self._stats:
                self._stats[proxy_url]['success'] += 1
                self._stats[proxy_url]['consecutive_fails'] = 0
                if proxy_url in self._failed_proxies:
                    self._failed_proxies.discard(proxy_url)
    
    def mark_fail(self, proxy_url):
        """标记请求失败"""
        with self._lock:
            if proxy_url in self._stats:
                self._stats[proxy_url]['fail'] += 1
                self._stats[proxy_url]['consecutive_fails'] += 1
                if self._stats[proxy_url]['consecutive_fails'] >= 3:
                    self._failed_proxies.add(proxy_url)


def _get_proxy_manager():
    """获取或初始化代理管理器单例"""
    global _proxy_manager_instance
    
    if _proxy_manager_instance is not None:
        return _proxy_manager_instance
    
    with _proxy_manager_lock:
        if _proxy_manager_instance is not None:
            return _proxy_manager_instance
        
        proxy_list_env = os.environ.get('PROXY_LIST', '')
        
        if not proxy_list_env:
            _proxy_manager_instance = None
            return None
        
        proxy_urls = [p.strip() for p in proxy_list_env.split(',') if p.strip()]
        
        if not proxy_urls:
            _proxy_manager_instance = None
            return None
        
        _proxy_manager_instance = ProxyManager(proxy_urls)
        return _proxy_manager_instance


def _get_aktools_url():
    """获取 AKTools URL"""
    return os.environ.get('AKTOOLS_URL')


def _aktools_stock_zh_index_daily(symbol):
    """AKTools 指数日线接口"""
    import pandas as pd
    aktools_url = _get_aktools_url()
    if not aktools_url:
        return None
    params = {'symbol': symbol}
    try:
        resp = requests.get(f'{aktools_url}/api/public/stock_zh_index_daily', params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)
        return df
    except:
        return None


def ak_with_fallback(ak_func, *args, aktools_func=None, **kwargs):
    """
    带有 AKTools 和代理回退的 akshare 调用封装
    """
    import logging
    logger = logging.getLogger(__name__)
    
    aktools_url = _get_aktools_url()
    proxy_mgr = _get_proxy_manager()
    
    # 尝试 1: AKTools
    if aktools_url and aktools_func:
        try:
            logger.info(f"AKTools: 尝试调用 {aktools_func.__name__}")
            result = aktools_func(*args, **kwargs)
            if result is not None and not (hasattr(result, 'empty') and result.empty):
                logger.info(f"AKTools: 成功获取数据")
                return result
            else:
                logger.warning(f"AKTools: 返回数据为空，尝试下一方案")
        except Exception as e:
            logger.warning(f"AKTools: 调用失败，尝试下一方案: {e}")
    
    # 尝试 2: 代理
    if proxy_mgr:
        try:
            logger.info(f"ProxyManager: 通过代理调用 {ak_func.__name__}")
            result = ak_func(*args, **kwargs)
            return result
        except Exception as e:
            logger.warning(f"代理模式失败，尝试直连: {e}")
    
    # 尝试 3: 直连
    logger.info(f"直连模式调用 {ak_func.__name__}")
    result = ak_func(*args, **kwargs)
    return result



logger = logging.getLogger(__name__)

class MarketValuationView(APIView):
    """市场估值视图"""
    permission_classes = [AllowAny]
    
    @method_decorator(cache_page(60 * 30))  # 缓存 30分钟
    def get(self, request):
        try:
            # 获取主要指数估值数据
            market_data = self._get_market_overview()
            index_data = self._get_index_data()
            industry_data = self._get_industry_valuation_csindex()
            sentiment_data = self._get_market_sentiment()
            
            market_valuation = {
                "market_overview": market_data,
                "index_data": index_data,
                "industry_valuation": industry_data,
                "market_sentiment": sentiment_data
            }
            
            return Response({
                "code": 0,
                "data": market_valuation,
                "message": "获取市场估值数据成功"
            })
            
        except Exception as e:
            return Response({
                "code": 1,
                "error": str(e),
                "message": "获取市场估值数据失败"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _get_market_overview(self):
        """获取市场整体估值情况"""
        try:
            # 获取最新的市场估值数据
            latest_valuations = MarketValuation.objects.order_by('-date').values()[:2]
            latest_valuation = latest_valuations[0]  # 最新一条记录
            previous_valuation = latest_valuations[1]  # 前一条记录
            
            # 比较上证指数趋势
            sh_trend = "↑" if latest_valuation.get("sh_index") > previous_valuation.get("sh_index") else ("↓" if latest_valuation.get("sh_index") < previous_valuation.get("sh_index") else "-")
            
            return {
                "pe": {
                    "ratio": latest_valuation.get("pe_ratio"),
                    "range_percentile": latest_valuation.get("pe_range_percentile"),
                },
                "pb": {
                    "ratio": latest_valuation.get("pb_ratio"),
                    "range_percentile": latest_valuation.get("pb_range_percentile"),
                },
                "dividend_ratio": latest_valuation.get("dividend_ratio"),
                "sh_index": {
                    "close": latest_valuation.get("sh_index"),
                    "trend": sh_trend,
                    "pe_rank_percentile": latest_valuation.get("sh_pe_rank_percentile"),
                },
                "trade_data": {
                    "total_volume": latest_valuation.get("total_volume"),
                    "total_amount": latest_valuation.get("total_amount"),
                },
                "date": latest_valuation.get("date").strftime("%Y-%m-%d")
            }
            
        except (MarketValuation.DoesNotExist, IndexData.DoesNotExist) as e:
            print(f"Error in _get_market_overview: {str(e)}")
            return {}
        
    def _get_index_data(self):
        """获取主要指数估值情况"""
        try:
            # 从数据库获取所有指数
            main_indices = IndexData.objects.exclude(
                code__in=['930903','000985']
            ).values(
                'code', 
                'name'
            ).distinct().order_by('code')

            # 获取这些日期的完整数据
            index_data = IndexData.objects.filter(
                code__in=[idx['code'] for idx in main_indices],
                date__in=models.Subquery(
                    IndexData.objects.filter(
                        code=models.OuterRef('code')
                    ).values('date').order_by('-date')[:2]
                )
            ).values(
                'code',
                'name',
                'date',
                'close',
                'pe_ttm_ratio',
                'percentile'
            ).order_by('code', '-date')  # 按代码升序、日期降序排列

            # 将数据重组为字典格式,便于访问
            index_dict = {}
            for item in index_data:
                if item['code'] not in index_dict:
                    index_dict[item['code']] = []
                index_dict[item['code']].append(item)

            # 处理每个指数的数据
            result = []
            for data_list in index_dict.values():  # 直接使用 values() 而不是 items()
                if len(data_list) >= 2:  # 确保有两天的数据
                    latest = data_list[0]
                    previous = data_list[1]
                    
                    # 比较大小，计算趋势
                    trend, sentiment = MarketValuationView._compare_market_trend(
                        latest['percentile'], 
                        previous['percentile']
                    )
                    
                    result.append({
                        'code': latest['code'],
                        'name': latest['name'],
                        'date': latest['date'],
                        'close': round(latest['close'], 2) if latest['close'] is not None else None,
                        'pe_ttm_ratio': round(latest['pe_ttm_ratio'], 2) if latest['pe_ttm_ratio'] is not None else None,
                        'percentile': round(latest['percentile'], 2) if latest['percentile'] is not None else None,
                        'trend': trend,
                        'sentiment': sentiment
                    })

            return result
        
        except Exception as e:
            return []
        
        
    def _get_industry_valuation_csindex(self):
        """获取中证行业估值情况"""
        try:
            # 获取最新日期
            latest_date = IndustryValuation.objects.latest('date').date

            # 查询最新日期的数据
            last_industry_csindex_data = IndustryValuation.objects.filter(
                industry_code__regex=r'^[A-S]',
                date=latest_date
            ).values('industry_name', 'ttm_pe', 'pb_ratio', 'dividend_ratio', 'stock_count')

            industry_data = []
            for row in last_industry_csindex_data:
                industry = {
                    "name": row["industry_name"],
                    "pe_ttm_ratio": float(row["ttm_pe"]) if row["ttm_pe"] is not None else None,
                    "pb_ratio": float(row["pb_ratio"]) if row["pb_ratio"] is not None else None,
                    # "dividend_ratio": float(row["dividend_ratio"]) if row["dividend_ratio"] is not None else None,
                    # "stock_count": int(row["stock_count"]) if row["stock_count"] is not None else None
                }
                industry_data.append(industry)
            
            return industry_data
        
        except Exception as e:
            return []
    
    def _get_market_sentiment(self):
        """获取市场情绪指标"""
        try:
            # 获取北向资金数据
            # north_money = ak.stock_hsgt_fund_flow_summary_em()
            # north_data = north_money[north_money['资金方向'] == '北向']
            
            # 从数据库获取两融数据
            latest_margin = MarginTradingData.objects.values(
                'date',
                'margin_balance',
                'securities_balance',
            ).latest('date')
            
            # 获取最新的市场估值数据，包含市场温度
            latest_valuation = MarketValuation.objects.latest('date')
            
            # 获取前一个交易日的市场估值数据，用于计算趋势
            previous_valuation = MarketValuation.objects.filter(
                date__lt=latest_valuation.date
            ).order_by('-date').first()
            
            # 计算温度趋势
            if previous_valuation and previous_valuation.market_temperature is not None and latest_valuation.market_temperature is not None:
                temperature_diff = latest_valuation.market_temperature - previous_valuation.market_temperature
                if temperature_diff > 0:
                    temperature_trend = "↑"
                elif temperature_diff < 0:
                    temperature_trend = "↓"
                else:
                    temperature_trend = "→"
            else:
                temperature_trend = "→"
            
            # 判断市场情绪状态
            if latest_valuation.market_temperature is None:
                market_sentiment = "Unknown"  # 未知状态
            elif latest_valuation.market_temperature <= 30:
                market_sentiment = "Low"  # 低估
            elif latest_valuation.market_temperature <= 70:
                market_sentiment = "Medium"  # 中估
            else:
                market_sentiment = "High"  # 高估
            
            # 获取 2024 年 12 月 2 日至今的市场温度数据
            temperature_history = MarketValuation.objects.filter(
                date__gte='2024-12-02'
            ).values(
                'date',
                'market_temperature'
            ).order_by('date')

            result = {
                "margin_trading": {
                    "margin_balance": float(latest_margin["margin_balance"]),
                    "margin_security_balance": float(latest_margin["securities_balance"]),
                    "margin_total": float(latest_margin["margin_balance"]) + float(latest_margin["securities_balance"]),
                    "date": latest_margin["date"].strftime("%Y-%m-%d")
                },
                "market_temperature": {
                    "temperature": latest_valuation.market_temperature,
                    "temperature_trend": temperature_trend,
                    "market_sentiment": market_sentiment
                },
                "temperature_trend": temperature_history
            }
            
            return result
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error in _get_market_sentiment: {str(e)}")
            # 返回默认结构，避免 KeyError
            return {
                "margin_trading": {
                    "margin_balance": 0.0,
                    "margin_security_balance": 0.0,
                    "margin_total": 0.0,
                    "date": "2025-07-11"
                },
                "market_temperature": {
                    "temperature": 0.0,
                    "temperature_trend": "→",
                    "market_sentiment": "Unknown"
                },
                "temperature_trend": []
            }

    @staticmethod
    def _compare_market_trend(latest_temperature, previous_temperature):
        """判断趋势及估值水平"""
        # 判断趋势上升还是下降
        # logger.info(f"latest_temperature: {latest_temperature}, previous_temperature: {previous_temperature}")
        diff = round(latest_temperature, 0) - round(previous_temperature, 0)

        if diff > 0:
            trend = "↑"
        elif diff < 0:
            trend = "↓"
        else:
            trend = "→"
        
        if latest_temperature <= 30:
            sentiment = "Low"  # 低估
        elif latest_temperature <= 70:
            sentiment = "Medium"  # 中估
        else:
            sentiment = "High"  # 高估

        return trend,sentiment


class MarketTrendView(APIView):
    """市场趋势视图"""
    permission_classes = [IsAuthenticated]

    @method_decorator(cache_page(60 * 30))  # 缓存 30分钟
    def get(self, request):
        try:
            # 获取主要指数走势
            indices_trend = self._get_indices_trend()
            # sector_performance = self._get_sector_performance()
            # style_performance = self._get_style_performance()
            
            market_trend = {
                "indices_trend": indices_trend,
                # "sector_performance": sector_performance,
                # "style_performance": style_performance
            }
            
            return Response({
                "code": 0,
                "data": market_trend,
                "message": "获取市场趋势数据成功"
            })
            
        except Exception as e:
            return Response({
                "code": 1,
                "error": str(e),
                "message": "获取市场趋势数据失败"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _get_indices_trend(self):
        """获取主要指数走势"""
        indices = {
            "sh000300": "沪深300",
            "sh000016": "上证50",
            "sh000905": "中证500",
            "sh000852": "中证1000"
        }
        
        result = {}
        for code, name in indices.items():
            df = ak_with_fallback(ak.stock_zh_index_daily, symbol=code, aktools_func=_aktools_stock_zh_index_daily)
            latest = df.iloc[-1]
            
            result[name] = {
                "close": float(latest["close"]),
                "volume": float(latest["volume"]),
                "date": latest.date.strftime("%Y-%m-%d")
            }
        
        return result
    
    def _get_sector_performance(self):
        """获取行业板块表现"""
        try:
            sector_data = ak.stock_sector_spot()
            
            sectors = []
            for idx, row in sector_data.iterrows():
                sector = {
                    "name": row["板块名称"],
                    "change": float(row["涨跌幅"]),
                    "turnover_rate": float(row["换手率"]),
                    "volume": float(row["成交量"]),
                }
                sectors.append(sector)
            
            return sectors
            
        except Exception as e:
            return []
    
    def _get_style_performance(self):
        """获取投资风格表现"""
        try:
            # 获取不同风格指数数据
            styles = {
                "sh000919": "300价值",
                "sh000918": "300成长",
                "sh000922": "中证红利",
                "sh000925": "基本50"
            }
            
            result = {}
            for code, name in styles.items():
                df = ak_with_fallback(ak.stock_zh_index_daily, symbol=code, aktools_func=_aktools_stock_zh_index_daily)
                latest = df.iloc[-1]
                
                result[name] = {
                    "close": float(latest["close"]),
                    "change": float(latest["change"]),
                    "date": latest.name.strftime("%Y-%m-%d")
                }
            
            return result
            
        except Exception as e:
            return {}


class BigDataInvestmentMarketDataView(APIView):
    """大数据投资市场数据视图"""
    permission_classes = [IsAuthenticated]
    
    @method_decorator(cache_page(60 * 30))  # 缓存 30分钟
    def get(self, request):
        try:
            # 获取大数据投资市场数据
            bd_market_data = self._get_bigdata_investment_market_data()
            # 大数据投资指数数据（ 000985 中证全指指数）
            bd_index_data = self._get_bigdata_investment_index_data()
            # 大数据投资股票池列表
            bd_stock_pool = self._get_bigdata_investment_stock_pool()

            bd_market_data = {
                "bd_market_data": bd_market_data,
                "bd_index_data": bd_index_data,
                "bd_stock_pool": bd_stock_pool
            }
            
            return Response({
                "code": 0,
                "data": bd_market_data,
                "message": "获取大数据投资市场数据成功"
            })
            
        except Exception as e:
            return Response({
                "code": 1,
                "error": str(e),
                "message": "获取大数据投资市场数据失败"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


    def _get_bigdata_investment_market_data(self):
        """获取大数据投资市场数据"""
        try:
            # 获取最新的市场估值数据
            latest_valuation = MarketValuation.objects.latest('date')

            pe_ratio = latest_valuation.pe_ratio
            pb_ratio = latest_valuation.pb_ratio
            pe_ttm_ratio = latest_valuation.pe_ttm_ratio
            dividend_ratio = latest_valuation.dividend_ratio

            # 计算合理仓位(以全市场实际极值计算)
            # 暂无沪深历史动态市盈率数据
            history_pe_min = 11.05
            history_pe_avg = 25.00
            history_pb_min = 1.25
            history_pb_avg = 2.50

            
            pex = 0.95 - (pe_ratio - history_pe_min)*0.45 / (history_pe_avg - history_pe_min)
            pbx = 0.95 - (pb_ratio - history_pb_min)*0.45 / (history_pb_avg - history_pb_min)

            # 计算合理仓位
            percentage_of_stocks = ((2*pex - 1) + (2*pbx - 1))/2


            return {
                "pe_ratio": pe_ratio,
                "pb_ratio": pb_ratio,
                "pe_ttm_ratio": pe_ttm_ratio,
                "dividend_ratio": dividend_ratio,
                "percentage_of_stocks": percentage_of_stocks,
                "date": latest_valuation.date.strftime("%Y-%m-%d")
            }
            
        except (MarketValuation.DoesNotExist, IndexData.DoesNotExist) as e:
            print(f"Error in _get_market_overview: {str(e)}")
            return {}
    

    def _get_bigdata_investment_index_data(self):
        """获取大数据投资指数数据"""
        try:
            # 获取最新日期
            latest_date = IndexData.objects.filter(code='000985').latest('date').date
            
            # 从 IndexData 数据库获取【000985 中证全指指数】周数据
            bd_index_data = IndexData.objects.filter(
                code='000985'
            ).annotate(
                weekday=Extract('date', 'iso_week_day')
            ).filter(
                models.Q(weekday=5) |  # 周五数据
                models.Q(date=latest_date)  # 当周最新数据
            ).values(
                'date',
                'pe_ttm_ratio',
            ).distinct().order_by('-date')

            return bd_index_data
        
        except Exception as e:
            return []

    def _get_bigdata_investment_stock_pool(self):
        """获取大数据投资股票池列表"""
        try:
            bd_stock_pool = BigDataStrategyStockData.objects.filter(
                models.Q(one_year_increase__lt=150) &  # 最近一年涨幅小于150%
                models.Q(bigdata_score__gt=80)  # 大数得分大于80
            ).values(
                'code',
                'name',
                'bigdata_score',
                'pe_ratio',
                'pe_ttm_ratio',
                'pb_ratio',
                'dividend_ratio',
                'one_year_increase',
                'total_market_value',
                'industry_name',
                'industry_code',
                'parent_industry_code',
            ).order_by('bigdata_score')

            return bd_stock_pool
        
        except Exception as e:
            return []
    

class ConvertibleBondMarketDataView(APIView):
    """可转债市场数据视图"""
    permission_classes = [IsAuthenticated]
    
    @method_decorator(cache_page(60 * 30))  # 缓存 30分钟
    def get(self, request):
        try:
            # 获取可转债市场数据
            cb_index_data = self._get_convertible_bond_index_data()
            # 可转债小盘策略
            cb_smallsize_data = self._get_convertible_bond_smallsize_data()
            # 可转债双低策略
            cb_doublelow_data = self._get_convertible_bond_doublelow_data()
            # 强赎可转债列表
            cb_redemption_bonds = self._get_redemption_convertible_bonds()

            convertible_bond_market_data = {
                "cb_index_data": cb_index_data,
                "cb_smallsize_data": cb_smallsize_data,
                "cb_doublelow_data": cb_doublelow_data,
                "cb_redemption_bonds": cb_redemption_bonds
            }
            
            return Response({
                "code": 0,
                "data": convertible_bond_market_data,
                "message": "获取可转债市场数据成功"
            })
            
        except Exception as e:
            return Response({
                "code": 1,
                "error": str(e),
                "message": "获取可转债市场数据失败"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        
    def _get_convertible_bond_index_data(self):
        """获取可转债指数数据"""
        try:
            # 获取最新日期
            latest_date = BondIndexData.objects.filter(code='CB_INDEX').latest('date').date
            
            # 从 BondIndexData 数据库获取【可转债指数】周数据
            cb_index_data = BondIndexData.objects.filter(
                code='CB_INDEX'
            ).annotate(
                weekday=Extract('date', 'iso_week_day')
            ).filter(
                models.Q(weekday=5) |  # 周五数据
                models.Q(date=latest_date)  # 当周最新数据
            ).values(
                'date',
                'price',
                'avg_price',
                'mid_price',
                'avg_premium_rt',
                'avg_ytm_rt'
            ).distinct().order_by('-date')

            # 获取最新两条数据
            cb_index_data_trend = BondIndexData.objects.filter(
                code='CB_INDEX'
            ).order_by('-date')[:2]
            
            # 比较大小，计算趋势 _compare_market_trend
            trend, sentiment = MarketValuationView._compare_market_trend(cb_index_data_trend[0].price, cb_index_data_trend[1].price)


            # 先将 QuerySet 转换为列表
            cb_index_data_list = list(cb_index_data)

            # 创建新的字典，包含原有数据和新增数据
            first_item = cb_index_data_list[0]
            updated_item = {
                **first_item,
                'trend': trend,
            }

            # 更新列表中的第一项
            cb_index_data_list[0] = updated_item

            return cb_index_data_list
        except Exception as e:
            return []
        

    def _get_convertible_bond_smallsize_data(self):
        """获取可转债小盘策略数据"""
        try:
            # 获取最新日期
            latest_date = BondData.objects.latest('date').date
            
            # 获取最新日期的所有可转债数据（非强赎）
            convertible_bond_data = BondData.objects.filter(
                date=latest_date
            ).annotate( # 创建一个字段，用于存储下一次强赎日期
                next_redeem_date=Case(
                    When(
                        is_callable='公告不强赎',
                        redemption_countdown__contains='重新计',
                        then=Cast(
                            Substr('redemption_countdown', 5, 10),  # 提取日期部分
                            output_field=DateField()
                        )
                    ),
                    default=Value(datetime.now().date() + timedelta(days=366)), # 默认下一次强赎日期至少为一年后，方便对日期进行比较
                    output_field=DateField()
                )
            ).exclude(
                models.Q(is_callable__in=['已公告强赎', '公告要强赎']) |
                models.Q(remaining_size__gt=5.0) |  # 规模大于 5 亿的过滤掉
                models.Q(listing_date__gte=datetime.now().date()) |  # 上市时间晚于当前日期
                models.Q(listing_date__isnull=True) |  #上市时间为空（待上市）
                models.Q(maturity_date__isnull=True) |  # 到期日为空
                models.Q(stock_price__lt=2) |  # 正股价小于2元
                # models.Q(stock_pb__lt=1) |  # 正股 pb 小于 1 （新增，待查看效果）
                models.Q(stock_price__isnull=True) |  # 正股价为空
                ~models.Q(bond_rating__startswith='A') |  # 评级是A及以下
                models.Q(bond_rating__isnull=True) |  # 评级为空
                models.Q(is_risk_excluded=True) |  # 手动标记风险
                models.Q(name__icontains='退') | # 名字中包含“退”
                # models.Q(double_low__gt=300) | # 双低值高于 300
                # models.Q(stock_pe_ttm_ratio__isnull=True) |  # 正股 TTM PE 为空
                # models.Q(stock_industry_pb_ratio_median__lt=models.F('stock_pb')) |  # 正股 PB 小于行业中位数PB
                # models.Q(stock_industry_pe_ttm_ratio_median__lt=models.F('stock_pe_ttm_ratio')) |  # 正股 TTM PE 小于行业中位数TTM PE
                # models.Q(next_redeem_date__lte=datetime.now().date() + timedelta(days=365)) | # 下一次强赎日期小于一年（PS：这个条件感觉不太合理。很多正在强赎倒计时的转债都在策略列表里）
                models.Q(maturity_date__lte=datetime.now().date() + timedelta(days=365))  # 到期时间小于一年的
            ).values(
                'code',
                'name',
                'close',
                'convertible_value',
                'premium_rate',
                'bond_rating',
                'remaining_size',
                'maturity_date',
                'stock_pe_ttm_ratio'
            ).order_by('code')  # 按代码排序
            
            return list(convertible_bond_data)
        
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"获取可转债市场数据时出错: {str(e)}", exc_info=True)
            return []
        
    
    def _get_convertible_bond_doublelow_data(self):
        """获取可转债双低策略数据"""
        try:
            # 获取最新日期的所有可转债数据（非强赎）
            convertible_bond_data = BondData.objects.exclude(
                models.Q(is_callable__in=['已公告强赎', '公告要强赎']) | # 过滤已公告强赎、公告要强赎
                models.Q(double_low__isnull=True) |  # 双低值为空
                models.Q(listing_date__gte=datetime.now().date()) | # 上市时间晚于当前日期
                models.Q(listing_date__isnull=True) | # 上市时间为空
                models.Q(maturity_date__isnull=True) | # 到期日为空
                models.Q(bond_rating__isnull=True) |  # 评级为空
                models.Q(stock_price__lt=2) | # 正股价小于2元
                models.Q(is_risk_excluded=True) | # 手动标记风险
                models.Q(name__icontains='退') # 名字中包含“退”
                # models.Q(stock_pb__lt=1)  # 正股 pb 小于 1 （新增，待查看效果）
            ).values(
                'code', 
                'name', 
                'double_low',
                'close',
                'convertible_value',
                'premium_rate',
                'remaining_size',
                'maturity_date',
                'bond_rating'
            ).order_by('double_low')[:20]
            
            return list(convertible_bond_data)
        except Exception as e:
            return []
    
    def _get_redemption_convertible_bonds(self):
        """已公告强赎和临近到期的可转债"""
        try:
            callable_bonds = BondData.objects.annotate(
                first_two_numbers=Cast(
                    Substr('redemption_countdown', 1, 2), # 创造一个字段，不会过滤数据。取 redemption_countdown 的前两位，针对内容为 “0/15 | 30“ 的情况
                    output_field=IntegerField()
                )
            ).filter(
                models.Q(is_callable__in=['已公告强赎', '公告要强赎', '已满足强赎条件']) | #过滤已公告强赎、公告要强赎、已满足强赎条件
                (
                    models.Q(redemption_countdown__regex=r'^\d{2}') & # 同时过滤强赎倒计时的转债（并的关系）
                    models.Q(first_two_numbers__gte=10) # 针对新创建的字段进行过滤，筛选数据
                )
            ).exclude(
                models.Q(last_trading_date__lt=datetime.now().date())  # 排除最后交易日早于今天的
            ).annotate(
                display_countdown=models.Case(
                    models.When(is_callable='公告要强赎', then=models.Value('公告要强赎')), # 如果是公告强赎，则不必显示倒计时进度了
                    default=models.F('redemption_countdown'), # 否则显示倒计时进度
                    output_field=models.CharField(),
                )
            ).values(
                'code',          # 代码
                'name',          # 名称
                'close',         # 现价
                'convertible_price',    # 转股价
                'is_callable',          # 是否可强赎
                'redemption_trigger_price',  # 强赎触发价
                'redemption_price',     # 强赎价
                'display_countdown',    # 使用新的字段替代 redemption_countdown
                'last_trading_date',    # 最后交易日
            ).order_by('code')
            
            return list(callable_bonds)
        except Exception as e:
            return []
        

class KimiChatView(View):
    name = "AI Assistant Chat" # 这将被用作链接文本
    permission_classes = [IsAuthenticated]

    def __init__(self):
        super().__init__()
        self.client = OpenAI(
            api_key=settings.KIMI_API_KEY,
            base_url="https://api.moonshot.cn/v1"
        )

    def chat_with_kimi(self, messages):
        try:
            response = self.client.chat.completions.create(
                model="moonshot-v1-8k",
                messages=[
                    {
                        "role": "system",
                        "content": "你是 Kimi，由 Moonshot AI 提供的人工智能助手，你擅长可转债投资策略，有丰富的可转债及股票市场知识。你可以联网查询各种可转债市场的数据。你会为用户提供安全，有帮助，准确的回答。同时，你会拒绝一切涉及恐怖主义，种族歧视，黄色暴力等问题的回答。请在回答中引用搜索结果的来源。",
                    },
                    *messages
                ],
                temperature=0.1,
                tools=[
                        {
                            "type": "builtin_function",
                            "function": {
                                "name": "$web_search",
                            },
                        }
                    ],
                # tool_choice="auto",
                stop=["stopF.I.R.E"],
                stream=True
            )

            for chunk in response:
                try:
                    if chunk.choices[0].delta.content:
                        yield f"data: {chunk.choices[0].delta.content}\n\n"
                    elif chunk.choices[0].delta.tool_calls:
                        for tool_call in chunk.choices[0].delta.tool_calls:
                            if tool_call.function.name == "$web_search":
                                try:
                                    search_results = json.loads(tool_call.function.arguments)
                                    yield f"data: [搜索结果] {json.dumps(search_results, ensure_ascii=False)}\n\n"
                                except json.JSONDecodeError as e:
                                    logging.error(f"搜索结果解析失败: {e}")
                                    yield f"data: [错误] 搜索结果解析失败\n\n"
                except AttributeError as e:
                    logging.error(f"处理响应块时出错: {e}")
                    continue

            yield "data: [DONE]\n\n"

        except Exception as e:
            error_message = f"data: {{\"error\": \"{str(e)}\"}}\n\n"
            yield error_message

    def post(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            messages = data.get("messages", [])
            
            if not messages:
                return error_response(1, "Messages are required")

            response = StreamingHttpResponse(
                self.chat_with_kimi(messages),  # 传递消息列表
                content_type='text/event-stream'
            )
            response['X-Accel-Buffering'] = 'no'
            response['Cache-Control'] = 'no-cache'
            response['Connection'] = 'keep-alive'
            return response
            
        except json.JSONDecodeError:
            return error_response(1, "Invalid JSON", 400)

