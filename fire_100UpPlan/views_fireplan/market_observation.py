import akshare as ak
import pandas as pd
import numpy as np
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from datetime import datetime, timedelta
from rest_framework.permissions import IsAuthenticated
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


class MarketValuationView(APIView):
    """市场估值视图"""
    permission_classes = [IsAuthenticated]
    
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
            
            # 使用最新日期获取完整数据
            index_data = IndexData.objects.filter(
                code__in=[idx['code'] for idx in main_indices],
                date=models.Subquery(
                    IndexData.objects.filter(
                        code=models.OuterRef('code')
                    ).values('date').order_by('-date')[:1]
                )
            ).values(
                'code',
                'name',
                'date',
                'close',
                'pe_ttm_ratio',
                'percentile'
            ).order_by('code')
            
            # 转换为列表并格式化数据
            result = []
            for item in index_data:
                result.append({
                    'code': item['code'],
                    'name': item['name'],
                    'date': item['date'],
                    'close': round(item['close'], 2) if item['close'] is not None else None,
                    'pe_ttm_ratio': round(item['pe_ttm_ratio'], 2) if item['pe_ttm_ratio'] is not None else None,
                    'percentile': round(item['percentile'], 2) if item['percentile'] is not None else None,
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
            
            # 获取最新两条市场温度数据
            market_temperatures = MarketValuation.objects.values(
                'date', 
                'market_temperature'
            ).order_by('-date')[:2]
            
            latest_market_temperature = market_temperatures[0]['market_temperature'] if market_temperatures else None
            previous_market_temperature = market_temperatures[1]['market_temperature'] if len(market_temperatures) > 1 else None
            
            # 比较两个温度，判断市场情绪
            trend, sentiment = self._compare_market_temperatures(latest_market_temperature, previous_market_temperature)

            return {
                "margin_trading": {
                    "margin_balance": float(latest_margin["margin_balance"]),
                    "margin_security_balance": float(latest_margin["securities_balance"]),
                    "margin_total": float(latest_margin["margin_balance"]) + float(latest_margin["securities_balance"]),
                    "date": latest_margin["date"].strftime("%Y-%m-%d")
                },
                "market_temperature": {
                    "temperature": float(latest_market_temperature),
                    "temperature_trend": trend,
                    "market_sentiment": sentiment,
                    "date": market_temperatures[0]['date'].strftime("%Y-%m-%d")
                }
            }
            
        except Exception as e:
            return {}

    @staticmethod
    def _compare_market_temperatures(latest_temperature, previous_temperature):
        """根据市场温度判断估值水平"""
        # 判断温度上升还是下降
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
            df = ak.stock_zh_index_daily(symbol=code)
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
                df = ak.stock_zh_index_daily(symbol=code)
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
            
            # 比较大小，计算趋势 _compare_market_temperatures
            trend, sentiment = MarketValuationView._compare_market_temperatures(cb_index_data_trend[0].price, cb_index_data_trend[1].price)


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
        """获取可转债市场数据"""
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
                models.Q(maturity_date__isnull=True) |  # 到期日为空
                models.Q(stock_price__lt=2) |  # 正股价小于2元
                # models.Q(stock_pb__lt=1) |  # 正股 pb 小于 1 （新增，待查看效果）
                models.Q(stock_price__isnull=True) |  # 正股价为空
                ~models.Q(bond_rating__startswith='A') |  # 评级是A及以下
                models.Q(bond_rating__isnull=True) |  # 评级为空
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
                models.Q(stock_price__lt=2) # 正股价小于2元
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
                        # "content": "你是 Kimi，由 Moonshot AI 提供的人工智能助手，你擅长可转债投资策略，有丰富的可转债及股票市场知识。你会为用户提供安全，有帮助，准确的回答。同时，你会拒绝一切涉及恐怖主义，种族歧视，黄色暴力等问题的回答。如果用户提到需要分析某个可转债或可转债代码，则请按照如下示例进行分析：领益智造主要从事精密功能件、结构件、模组及充电器等业务。转债质地：信用评级AA+，税后年化1.63%，净资产收益率3%，毛利率15%，资产负债率52%。市盈率51市净率3.3，发行规模21.374亿。个人看法：最新转股价值96，期望115元左右上市。"
                        "content": "你是 Kimi，由 Moonshot AI 提供的人工智能助手，你擅长可转债投资策略，有丰富的可转债及股票市场知识。你会为用户提供安全，有帮助，准确的回答。同时，你会拒绝一切涉及恐怖主义，种族歧视，黄色暴力等问题的回答。",
                    },
                    *messages,
                    {
                        "role": "assistant",
                        "content": " ",
                        "partial": True
                    }
                ],
                temperature=0.1,
                stop=["stopF.I.R.E"],
                stream=True
            )

            for chunk in response:
                if hasattr(chunk.choices[0].delta, 'content'):
                    if chunk.choices[0].delta.content is not None:
                        yield f"data: {chunk.choices[0].delta.content}\n\n"

            yield "data: [DONE]\n\n"

        except Exception as e:
            error_message = f"data: {{\"error\": \"{str(e)}\"}}\n\n"
            yield error_message

    def post(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            messages = data.get("messages", [])
            
            if not messages:
                return JsonResponse(
                    {"error": "Messages are required"}, 
                    status=400
                )

            response = StreamingHttpResponse(
                self.chat_with_kimi(messages),  # 传递消息列表
                content_type='text/event-stream'
            )
            response['X-Accel-Buffering'] = 'no'
            response['Cache-Control'] = 'no-cache'
            response['Connection'] = 'keep-alive'
            return response
            
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)

