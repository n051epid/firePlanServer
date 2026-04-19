from datetime import datetime, timedelta, date
import threading
import akshare as ak
import pandas as pd
from django.db import transaction
from ..models import MarketValuation, IndexData, MarginTradingData, IndustryValuation, StockData, BondData, BondIndexData,BigDataStrategyStockData, StockHistoryData
import requests
import zipfile
import io
import os
import numpy as np
from django.db.models import Min, Max, F
from django.conf import settings
import logging
import re
from scipy import stats
import json
from playwright.sync_api import sync_playwright
import time


# ========================================
# Proxy + AKTools 系统
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
        
        logger.info(f"ProxyManager: 初始化完成，代理数量: {len(proxy_urls)}")
    
    def get_proxy(self):
        """获取下一个可用代理"""
        with self._lock:
            if self._direct_mode or not self._proxies:
                return None
            
            available = [p for p in self._proxies if p not in self._failed_proxies]
            if not available:
                logger.warning("ProxyManager: 所有代理失效，回退到直连模式")
                self._direct_mode = True
                return None
            
            proxy = available[self._current_index % len(available)]
            self._current_index += 1
            # 同时设置 http 和 https 代理
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
                    logger.info(f"ProxyManager: 代理恢复成功: {proxy_url}")
    
    def mark_fail(self, proxy_url):
        """标记请求失败"""
        with self._lock:
            if proxy_url in self._stats:
                self._stats[proxy_url]['fail'] += 1
                self._stats[proxy_url]['consecutive_fails'] += 1
                if self._stats[proxy_url]['consecutive_fails'] >= 3:
                    self._failed_proxies.add(proxy_url)
                    logger.warning(f"ProxyManager: 代理连续失败3次，标记失效: {proxy_url}")
    
    def request_with_proxy(self, method, url, **kwargs):
        """使用代理发送请求，自动切换失效代理"""
        max_retries = len([p for p in self._proxies if p not in self._failed_proxies]) + 1
        attempts = 0
        
        while attempts < max_retries:
            proxy = self.get_proxy()
            proxy_url = proxy['http'] if proxy else None
            
            try:
                if method.lower() == 'get':
                    if proxy:
                        kwargs['proxies'] = proxy
                    response = self._original_get(url, **kwargs)
                else:
                    if proxy:
                        kwargs['proxies'] = proxy
                    response = self._original_post(url, **kwargs)
                
                if proxy_url:
                    self.mark_success(proxy_url)
                return response
                
            except Exception as e:
                if proxy_url:
                    self.mark_fail(proxy_url)
                
                attempts += 1
                if attempts >= max_retries:
                    # 最后尝试直连
                    logger.warning(f"ProxyManager: 代理全部失败，尝试直连")
                    try:
                        kwargs.pop('proxies', None)
                        if method.lower() == 'get':
                            return self._original_get(url, **kwargs)
                        else:
                            return self._original_post(url, **kwargs)
                    except:
                        raise
                else:
                    logger.warning(f"ProxyManager: 代理 {proxy_url} 失败，切换下一个: {e}")


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
            logger.info("ProxyManager: 未配置 PROXY_LIST，使用直连模式")
            _proxy_manager_instance = None
            return None
        
        proxy_urls = [p.strip() for p in proxy_list_env.split(',') if p.strip()]
        
        if not proxy_urls:
            logger.info("ProxyManager: PROXY_LIST 为空，使用直连模式")
            _proxy_manager_instance = None
            return None
        
        _proxy_manager_instance = ProxyManager(proxy_urls)
        return _proxy_manager_instance


def _get_aktools_url():
    """获取 AKTools URL"""
    return os.environ.get('AKTOOLS_URL')


def _aktools_stock_zh_a_pe(symbol):
    """AKTools PE接口"""
    aktools_url = _get_aktools_url()
    if not aktools_url:
        return None
    params = {'symbol': symbol}
    try:
        resp = requests.get(f'{aktools_url}/api/public/stock_zh_a_pe', params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)
        return df
    except:
        return None


def _aktools_stock_zh_a_pb(symbol):
    """AKTools PB接口"""
    aktools_url = _get_aktools_url()
    if not aktools_url:
        return None
    params = {'symbol': symbol}
    try:
        resp = requests.get(f'{aktools_url}/api/public/stock_zh_a_pb', params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)
        return df
    except:
        return None


def _aktools_stock_zh_index_daily(symbol):
    """AKTools 指数日线接口"""
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


def _aktools_stock_zh_a_spot_em():
    """AKTools A股实时行情接口"""
    aktools_url = _get_aktools_url()
    if not aktools_url:
        return None
    try:
        resp = requests.get(f'{aktools_url}/api/public/stock_zh_a_spot_em', timeout=30)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)
        return df
    except:
        return None


def ak_with_fallback(ak_func, *args, aktools_func=None, **kwargs):
    """
    带有 AKTools 和代理回退的 akshare 调用封装
    
    优先级：
    1. AKTools API（如果配置了 AKTOOLS_URL 且提供了 aktools_func）
    2. 代理轮询（如果配置了 PROXY_LIST）
    3. 直连 akshare（原有行为）
    """
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



class MarketDataFetcher:
    @staticmethod
    def safe_float(value):
        """安全转换为浮点数，处理特殊值"""
        if pd.isna(value) or value == '-' or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def safe_int(value):
        """安全转换为整数，处理特殊值"""
        if pd.isna(value) or value == '-' or value == '':
            return None
        try:
            return int(float(value))  # 先转float再转int，处理科学记数法
        except (ValueError, TypeError):
            return None

    @staticmethod
    def fetch_daily_market_data(start_date=None, end_date=None):
        """获取市场整体估值数据"""
        logger.info(f"Fetcher: Fetching daily market data for date: {start_date} to {end_date}")
        
        # 定义获取市场整体估值数据函数
        def get_stock_market_pe_pb(px, symbol):
            logger.info(f"Fetcher: Getting {px} data for {symbol}")
            if px == 'pe':
                try:
                    data = ak_with_fallback(ak.stock_market_pe_lg, symbol=symbol, aktools_func=_aktools_stock_zh_a_pe)
                    return data
                except Exception as e:
                    logger.error(f"get_stock_market_pe Error: {e}")
                    logger.info(f"Restart celery service")
                    # 重启celery服务：sudo systemctl restart celery_worker_fireplan.service
                    if os.environ.get('SERVER_MODE') == 'release':
                        os.system('sudo systemctl restart celery_worker_fireplan_release.service')
                    else:
                        os.system('sudo systemctl restart celery_worker_fireplan.service')
                    raise Exception(f"Failed to fetch data,restart celery service")
            elif px == 'pb':
                try:
                    data = ak_with_fallback(ak.stock_market_pb_lg, symbol=symbol, aktools_func=_aktools_stock_zh_a_pb)
                    return data
                except Exception as e:
                    logger.error(f"get_stock_market_pb Error: {e}")
                    logger.info(f"Restart celery service")
                    # 重启celery服务：sudo systemctl restart celery_worker_fireplan.service
                    if os.environ.get('SERVER_MODE') == 'release':
                        os.system('sudo systemctl restart celery_worker_fireplan_release.service')
                    else:
                        os.system('sudo systemctl restart celery_worker_fireplan.service')
                    raise Exception(f"Failed to fetch data,restart celery service")

        # 获取上证指数估值数据
        try:
            # 添加请求头
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            if start_date and end_date:
                # 如果传入的是字符串格式 'YYYYMMDD'，需要指定格式
                if isinstance(start_date, str):
                    start_date = pd.to_datetime(start_date, format='%Y%m%d')
                else:
                    start_date = pd.to_datetime(start_date)
                if isinstance(end_date, str):
                    end_date = pd.to_datetime(end_date, format='%Y%m%d')
                else:
                    end_date = pd.to_datetime(end_date)
            else:
                start_date = pd.to_datetime(datetime.now().strftime('%Y%m%d'), format='%Y%m%d')
                end_date = start_date

            date_list = []
            current_date = start_date
            while current_date <= end_date:
                if current_date.weekday() < 5:  # 0-4 表示周一到周五
                    date_list.append(current_date.strftime('%Y%m%d'))
                # 使用 pd.Timedelta 或转换为 datetime 后使用 timedelta
                current_date = current_date + pd.Timedelta(days=1)
            
            if len(date_list) == 0:
                return {'status': 'error', 'message': 'No date list'}
            else:
                success_count = 0
                fail_count = 0
                sh_pe = get_stock_market_pe_pb('pe','上证')
                sh_pb = get_stock_market_pe_pb('pb','上证')
                
                # 获取上证指数行情
                try:
                    sh_index = ak_with_fallback(ak.stock_zh_index_daily, symbol="sh000001", aktools_func=_aktools_stock_zh_index_daily)
                    if sh_index.empty:
                        logger.error("Failed to fetch Shanghai index data")
                        return {'status': 'error', 'message': 'Failed to fetch Shanghai index data'}
                except Exception as e:
                    logger.error(f"Error fetching Shanghai index: {str(e)}")
                    return {'status': 'error', 'message': f'Error fetching Shanghai index: {str(e)}'}
                
                sh_index['date'] = pd.to_datetime(sh_index['date'])
                
                # 获取东方财富网-沪深京 A 股-实时行情数据(只能是最新的，无法指定日期，仅用来计算全市场交易量和交易额)
                market_data = ak_with_fallback(ak.stock_zh_a_spot_em, aktools_func=_aktools_stock_zh_a_spot_em)
                total_volume = market_data['成交量'].sum()
                total_amount = market_data['成交额'].sum()

                for date in date_list:
                    try:
                        today = date
                        logger.info(f"处理日期: {today}")
                        
                        # 验证当天是否有上证指数数据
                        try:
                            today_date = pd.to_datetime(today, format='%Y%m%d').date()
                        except:
                            # 如果转换失败，尝试其他格式
                            today_date = pd.to_datetime(today).date()
                        
                        # 比较日期部分（不比较时间）
                        latest_sh_data = sh_index[sh_index['date'].dt.date == today_date]
                        
                        # 如果当天没有数据，跳过该日期
                        if latest_sh_data.empty:
                            logger.warning(f"日期 {today} 没有上证指数数据，跳过")
                            fail_count += 1
                            continue
                        
                        # 仅用于更新本条数据的sh_index字段，后续不会使用
                        latest_sh_index = latest_sh_data['close'].iloc[0]
                        
                        # 计算12年前的日期
                        current_date = pd.Timestamp(today)
                        twelve_years_ago = current_date - pd.DateOffset(years=12)
                        
                        # 添加数据验证
                        dates = pd.to_datetime(sh_pe['日期'])
                        latest_12_sh_pe_valuation = sh_pe[dates >= twelve_years_ago]
                        if latest_12_sh_pe_valuation.empty:
                            return {
                                'status': 'error',
                                'message': 'No PE data found for the last 12 years'
                            }
                        dates = pd.to_datetime(sh_pb['日期'])
                        latest_12_sh_pb_valuation = sh_pb[dates >= twelve_years_ago]
                        if latest_12_sh_pb_valuation.empty:
                            return {
                                'status': 'error',
                                'message': 'No PB data found for the last 12 years'
                            }
                        
            
                        pe_series = latest_12_sh_pe_valuation["平均市盈率"]
                        pb_series = latest_12_sh_pb_valuation["市净率"]
                        
                        # 添加数据验证
                        if pe_series.empty or pb_series.empty:
                            return {
                                'status': 'error',
                                'message': 'PE or PB series is empty'
                            }
                        
                        # 添加数据验证
                        try:
                            current_sh_pe = float(pe_series.iloc[-1])
                            current_sh_pb = float(pb_series.iloc[-1])
                        except (IndexError, ValueError) as e:
                            return {
                                'status': 'error',
                                'message': f'Error processing PE/PB values: {str(e)}'
                            }
                        
                        # 添加数据验证
                        try:
                            sh_pe_rank = len(pe_series[pe_series <= current_sh_pe]) / len(pe_series) * 100
                            sh_pb_rank = len(pb_series[pb_series <= current_sh_pb]) / len(pb_series) * 100
                        except Exception as e:
                            return {
                                'status': 'error',
                                'message': f'Error calculating PE/PB ranks: {str(e)}'
                            }

                        # 通过下载表格，获取全市场的估值数据
                        # today = '20241212'
                        # url = f'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/dl_resource/industry_pe/bk{today}.zip'
                        url = f'https://oss-ch.csindex.com.cn/dl_resource/industry_pe/bk{today}.zip'
                        logger.info(f"Fetcher: url: {url}")
                        # 沪深市场，不包括北交所（京市），近 12 年历史估值区间（2012-01-01 至 2024-11-16）。数据来源：https://legulegu.com/stockdata/a-pe。
                        # 暂无沪深历史动态市盈率数据
                        # history_pe_min = 11.05
                        # history_pe_max = 29.95
                        # history_pb_min = 1.25
                        # history_pb_max = 3.52
                        
                        # 从数据库获取历史极值
                        market_stats = MarketValuation.objects.aggregate(
                            min_pe=Min('pe_ratio'),
                            max_pe=Max('pe_ratio'),
                            min_pb=Min('pb_ratio'),
                            max_pb=Max('pb_ratio')
                        )

                        # 更新历史区间值（使用数据库值和默认值中的较小/较大值）
                        history_pe_min = min(market_stats['min_pe'] or float('inf'), 11.05)
                        history_pe_max = max(market_stats['max_pe'] or 0, 29.95)
                        history_pb_min = min(market_stats['min_pb'] or float('inf'), 1.25)
                        history_pb_max = max(market_stats['max_pb'] or 0, 3.52)

                        # 添加更多的错误处理和日志记录
                        try:
                            response = requests.get(url, headers=headers)
                            response.raise_for_status()  # 检查响应状态
                            if not response.content:
                                raise ValueError("Empty response received")
                            
                            # 记录响应内容用于调试
                            logger.debug(f"Response content: {response.content[:200]}...")  # 只记录前200个字符
                            
                        except requests.exceptions.RequestException as e:
                            logger.error(f"Request failed: {str(e)}")
                            return {'status': 'error', 'message': f'Request failed: {str(e)}'}
                        
                        if response.status_code == 200:

                            # 获取数据中沪深市场近 12 年的数据（PE、PB）以便于计算估值 Rank 百分位
                            twelve_years_ago = datetime.now() - timedelta(days=365 * 12)
                            historical_market_data = MarketValuation.objects.filter(
                                sector_name='沪深市场',
                                date__gte=twelve_years_ago
                            ).values('date', 'pe_ratio', 'pb_ratio').order_by('date')

                            # 转换为 DataFrame 以便计算百分位
                            if historical_market_data:
                                df_historical = pd.DataFrame(list(historical_market_data))  # 添加 list() 转换

                            # 解压zip文件
                            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                                # 获取zip中的xls文件名
                                xls_filename = zip_ref.namelist()[0]
                                # 创建临时目录
                                temp_dir = '/tmp/market_data'
                                os.makedirs(temp_dir, exist_ok=True)
                                # 解压到临时目录
                                zip_ref.extract(xls_filename, temp_dir)
                                
                                # 读取Excel文件的不同sheet
                                xls_path = os.path.join(temp_dir, xls_filename)
                                
                                # 读取静态市盈率sheet，计算PE区间百分位
                                df_static = pd.read_excel(xls_path, sheet_name='板块静态市盈率')
                                market_row = df_static[df_static['板块名称'] == '沪深市场'].iloc[0]
                                pe_ratio = round(float(market_row['最新静态\n市盈率']), 2)

                                # 更新历史区间值并计算区间百分位
                                history_pe_min = min(history_pe_min, pe_ratio)
                                history_pe_max = max(history_pe_max, pe_ratio)
                                pe_range_percentile = 0 if pe_ratio <= history_pe_min else (
                                    100 if pe_ratio >= history_pe_max else
                                    (pe_ratio - history_pe_min) / (history_pe_max - history_pe_min) * 100
                                )
                                # 计算当前 PE 在历史数据中的 Rank 百分位
                                current_pe = pe_ratio
                                pe_rank = len(df_historical[df_historical['pe_ratio'] <= current_pe]) / len(df_historical) * 100
                                pe_rank = round(pe_rank, 2)

                                # 读取滚动市盈率sheet
                                df_pe_ttm = pd.read_excel(xls_path, sheet_name='板块滚动市盈率')
                                market_row = df_pe_ttm[df_pe_ttm['板块名称'] == '沪深市场'].iloc[0]
                                pe_ttm_ratio = round(float(market_row['最新滚动\n市盈率']), 2)

                                # 读取市净率sheet，计算PB区间百分位
                                df_pb = pd.read_excel(xls_path, sheet_name='板块市净率')
                                market_row = df_pb[df_pb['板块名称'] == '沪深市场'].iloc[0]
                                pb_ratio = round(float(market_row['最新市净率']), 2)
                                
                                # 更新历史区间值并计算区间百分位
                                history_pb_min = min(history_pb_min, pb_ratio)
                                history_pb_max = max(history_pb_max, pb_ratio)
                                pb_range_percentile = 0 if pb_ratio <= history_pb_min else (
                                    100 if pb_ratio >= history_pb_max else
                                    (pb_ratio - history_pb_min) / (history_pb_max - history_pb_min) * 100
                                )
                                # 计算当前 PB 在历史数据中的 Rank 百分位
                                current_pb = pb_ratio
                                pb_rank = len(df_historical[df_historical['pb_ratio'] <= current_pb]) / len(df_historical) * 100
                                pb_rank = round(pb_rank, 2)

                                # 读取股息率sheet
                                df_dividend = pd.read_excel(xls_path, sheet_name='板块股息率')
                                market_row = df_dividend[df_dividend['板块名称'] == '沪深市场'].iloc[0]
                                dividend_ratio = round(float(market_row['最新股息率']), 2)
                                
                                # 获取沪深市场数据
                                stock_count = int(market_row['股票家数'])

                                # 获取最新市场温度数据之一(930903: 中证A股,000985: 中证全指)
                                market_temperatures_index = IndexData.objects.filter(
                                    code='000985',
                                    date=today
                                ).values(
                                    'percentile'
                                )

                                # 平权计算三个指标（全市场 PE_Rank、全市场 PB_Rank、指数温度）
                                if market_temperatures_index:
                                    logger.info('==================== 有指数温度数据 ====================')
                                    market_temperature = (pe_rank + pb_rank + market_temperatures_index[0]['percentile'])/3
                                else:
                                    # 如果没有指数温度数据，只使用 PE 和 PB 的 rank
                                    logger.warning('==================== 没有指数温度数据 ====================')
                                    market_temperature = (pe_rank + pb_rank)/2
                                
                                # 清空临时文件
                                os.remove(xls_path)
                        
                        
                        
                        # 创建并保存 MarketValuation 实例
                        with transaction.atomic():
                            valuation = MarketValuation(
                                date=today,
                                sector_name='沪深市场',
                                pe_ratio=pe_ratio,
                                pe_range_percentile=round(pe_range_percentile, 2),
                                pe_rank_percentile=round(pe_rank, 2),
                                pe_ttm_ratio=pe_ttm_ratio,
                                pb_ratio=pb_ratio,
                                pb_range_percentile=round(pb_range_percentile, 2),
                                pb_rank_percentile=round(pb_rank, 2),
                                dividend_ratio=dividend_ratio,
                                stock_count=stock_count,
                                total_volume=total_volume,
                                total_amount=total_amount,
                                market_temperature=round(market_temperature, 2),
                                sh_index=latest_sh_index,
                                sh_pe_rank_percentile=round(sh_pe_rank, 2),
                                sh_pb_rank_percentile=round(sh_pb_rank, 2),
                                created_at=datetime.now()
                            )
                            
                            # 检查是否已存在同一天的数据
                            existing = MarketValuation.objects.filter(date=today).first()
                            if existing:
                                for field in ['pe_ratio', 'market_temperature', 'pe_range_percentile', 'pe_rank_percentile', 'pb_ratio', 'pb_range_percentile', 'pb_rank_percentile', 'pe_ttm_ratio', 'dividend_ratio', 'stock_count', 'total_volume', 'total_amount', 
                                            'sh_index', 'sh_pe_rank_percentile', 'sh_pb_rank_percentile']:
                                    setattr(existing, field, getattr(valuation, field))
                                existing.save()
                                logger.info(f"Fetcher: Market valuation data existing and updated for date {today}.")
                                success_count += 1
                                continue  # 继续处理下一个日期
                            else:
                                valuation.save()
                                logger.info(f"Fetcher: New market valuation data created for date {today}.")
                                success_count += 1
                                continue  # 继续处理下一个日期
                                
                    except Exception as e:
                        logger.error(f"Error in fetch_daily_market_data for date {today}: {str(e)}")
                        fail_count += 1
                        continue  # 继续处理下一个日期
                
                # 循环结束后返回汇总结果
                return {
                    'status': 'completed',
                    'total': len(date_list),
                    'success': success_count,
                    'failed': fail_count,
                    'message': f'处理完成: 成功 {success_count} 天, 失败 {fail_count} 天'
                }
                    
        except Exception as e:
            logger.error(f"Error in fetch_daily_market_data: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error fetching market valuation: {str(e)}"
            }

    
    @staticmethod
    def fetch_margin_trading_data():
        """获取两融数据"""
        try:
            # 获取两融数据
            df = ak.stock_margin_account_info()
            
            if df.empty:
                print("No margin trading data fetched")
                return None
                
            # 转换日期格式
            df['日期'] = pd.to_datetime(df['日期'])
            latest_date = df['日期'].max()
            
            # 获取最新一条数据
            latest_data = df[df['日期'] == latest_date].iloc[0]
            
            # 数据类型转换和空值处理
            def safe_float(value, default=0.0):
                try:
                    return float(value) if pd.notna(value) else default
                except (ValueError, TypeError):
                    return default
            
            margin_data = MarginTradingData(
                date=latest_date,
                margin_balance=safe_float(latest_data.get('融资余额')),
                securities_balance=safe_float(latest_data.get('融券余额')),
                margin_buy=safe_float(latest_data.get('融资买入额')),
                securities_sell=safe_float(latest_data.get('融券卖出额')),
                securities_firms=safe_float(latest_data.get('证券公司数量')),
                branches=safe_float(latest_data.get('营业部数量')),
                individual_investors=safe_float(latest_data.get('个人投资者数量')),
                institutional_investors=safe_float(latest_data.get('机构投资者数量')),
                active_traders=safe_float(latest_data.get('参与交易的投资者数量')),
                margin_traders=safe_float(latest_data.get('有融资融券负债的投资者数量')),
                collateral_value=safe_float(latest_data.get('担保物总价值')),
                maintenance_ratio=safe_float(latest_data.get('平均维持担保比例'))
            )

            # # 使用 get_or_create 避免重复数据
            with transaction.atomic():
                existing_data = MarginTradingData.objects.filter(date=latest_date).first()
                if existing_data:
                    # 更新现有记录
                    for field in ['margin_balance', 'securities_balance', 'margin_buy', 
                                'securities_sell', 'securities_firms', 'branches', 
                                'individual_investors', 'institutional_investors', 
                                'active_traders', 'margin_traders', 'collateral_value', 
                                'maintenance_ratio']:
                        setattr(existing_data, field, getattr(margin_data, field))
                    existing_data.save()
                    return True
                else:
                    # 创建新记录
                    margin_data.save()
                    return True
                    
        except Exception as e:
            print(f"Error fetching margin trading data: {str(e)}")
            return None

    # 处理行业估值数据
    @staticmethod
    def process_industry_valuation(df, sheet_type='static_pe'):
        """处理行业估值数据"""
        records = []
        current_parent = None


        
        for _, row in df.iterrows():
            industry_code = str(row['行业代码']).strip() if pd.notna(row['行业代码']) else None
            industry_name = str(row['行业名称']).strip().replace('、', '\\')
            
            # 跳过空行
            if not industry_name or industry_name == '-':
                continue
                
            # 判断是否为父级行业（例如 A、B 等）
            if len(str(industry_code)) == 1:
                current_parent = industry_name
                parent_industry = None
            else:
                parent_industry = current_parent
            
            # 获取值并处理空值情况
            value = row.get('最新静态\n市盈率' if sheet_type == 'static_pe' else
                          '最新滚动\n市盈率' if sheet_type == 'ttm_pe' else
                          '最新市净率' if sheet_type == 'pb_ratio' else
                          '最新股息率')
            
            # 处理特殊值
            if pd.isna(value) or value == '-' or value == '':
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None


            # 处理股票家数
            try:
                stock_count = int(row['股票家数']) if pd.notna(row['股票家数']) and row['股票家数'] != '-' else 0
            except (ValueError, TypeError):
                stock_count = 0
                
            records.append({
                'industry_code': industry_code,
                'industry_name': industry_name,
                'parent_industry': parent_industry,
                'value': value,
                'stock_count': stock_count
            })
            
        return records
    
    @staticmethod
    def process_stock_data(df):
        """处理个股数据"""
        records = []
        
        for _, row in df.iterrows():
            try:
                code = str(row['证券代码']).strip()
                name = str(row['证券名称']).strip()
                industry_name = str(row['中上协大类名称']).strip()
                industry_code = str(row['中上协大类代码']).strip()
                parent_industry_code = str(row['中上协门类代码']).strip()

                # 确保基本价格数据有默认值
                record = {
                    'code': code,
                    'name': name,
                    'industry_name': industry_name,
                    'industry_code': industry_code,
                    'parent_industry_code': parent_industry_code,
                    'open': 0.0,  # 设置默认值
                    'high': 0.0,
                    'low': 0.0,
                    'close': 0.0,
                    'volume': 0,
                    'amount': 0.0,
                    'pe_ratio': MarketDataFetcher.safe_float(row['个股静态市盈率']),
                    'pe_ttm_ratio': MarketDataFetcher.safe_float(row['个股滚动市盈率']),
                    'pb_ratio': MarketDataFetcher.safe_float(row['个股市净率']),
                    'dividend_ratio': MarketDataFetcher.safe_float(row['个股股息率'])
                }
                
                records.append(record)
            except Exception as e:
                print(f"Error processing row: {str(e)}")
                continue

        return records

    @staticmethod
    def get_sheet_name(xls_path, keyword):
        """获取包含特定关键词的sheet名称"""
        xl = pd.ExcelFile(xls_path)
        sheet_names = xl.sheet_names
        
        # 查找包含关键词的sheet名
        for sheet in sheet_names:
            if keyword in sheet:
                return sheet
        return None

            
    # 获取行业估值数据：https://www.csindex.com.cn/#/dataService/PERatio
    @staticmethod
    def fetch_industry_valuation(date):
        """获取行业估值数据"""
        logger = logging.getLogger(__name__)

        try:
            # today = datetime.now().strftime('%Y%m%d')
            today = date
            # url = f'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/dl_resource/industry_pe/{today}.zip'
            url = f'https://oss-ch.csindex.com.cn/dl_resource/industry_pe/{today}.zip'
            logger.info(f"Fetcher: 获取股票数据估值数据（CSI）: {url}")

            response = requests.get(url)
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                    xls_filename = zip_ref.namelist()[0]
                    temp_dir = '/tmp/market_data'
                    os.makedirs(temp_dir, exist_ok=True)
                    zip_ref.extract(xls_filename, temp_dir)
                    
                    xls_path = os.path.join(temp_dir, xls_filename)
                    
                    # 获取各sheet名称
                    static_pe_sheet = MarketDataFetcher.get_sheet_name(xls_path, '静态市盈率')
                    pe_ttm_sheet = MarketDataFetcher.get_sheet_name(xls_path, '滚动市盈率')
                    pb_sheet = MarketDataFetcher.get_sheet_name(xls_path, '市净率')
                    dividend_sheet = MarketDataFetcher.get_sheet_name(xls_path, '股息率')
                    stock_sheet = MarketDataFetcher.get_sheet_name(xls_path, '个股数据')
                    
                    if not all([static_pe_sheet, pe_ttm_sheet, pb_sheet, dividend_sheet, stock_sheet]):
                        raise ValueError("无法找到所需的sheet")
                    
                    # 读取各sheet的数据
                    df_pe_static = pd.read_excel(xls_path, sheet_name=static_pe_sheet)
                    df_pe_ttm = pd.read_excel(xls_path, sheet_name=pe_ttm_sheet)
                    df_pb = pd.read_excel(xls_path, sheet_name=pb_sheet)
                    df_dividend = pd.read_excel(xls_path, sheet_name=dividend_sheet)
                    df_stock = pd.read_excel(xls_path, sheet_name=stock_sheet, dtype={'证券代码': str})
                    
                    # 处理各类型数据
                    static_pe_data = MarketDataFetcher.process_industry_valuation(df_pe_static, 'static_pe')
                    ttm_pe_data = MarketDataFetcher.process_industry_valuation(df_pe_ttm, 'ttm_pe')
                    pb_data = MarketDataFetcher.process_industry_valuation(df_pb, 'pb_ratio')
                    dividend_data = MarketDataFetcher.process_industry_valuation(df_dividend, 'dividend_ratio')
                    stock_data = MarketDataFetcher.process_stock_data(df_stock)

                    # 获取股票所在行业的估值情况
                    last_12_years_ago = datetime.now() - timedelta(days=365 * 12)
                    industry_history_data = IndustryValuation.objects.filter(
                            industry_code__regex=r'^[A-S]',
                            date__gte=last_12_years_ago
                        ).values('industry_code', 'ttm_pe', 'pb_ratio')
                    
                    # 计算 ttm_pe 的分布
                    pe_ttm_stats = MarketDataFetcher.calculate_normal_distribution(industry_history_data, 'ttm_pe')

                    # 计算 pb_ratio 的分布
                    pb_stats = MarketDataFetcher.calculate_normal_distribution(industry_history_data, 'pb_ratio')


                    # 合并数据并保存
                    # latest_date = datetime.now().date()
                    latest_date = date
                    with transaction.atomic():
                        for record in static_pe_data:
                            industry_code = record['industry_code']
                            if not industry_code:
                                continue
                                
                            # 查找或创建记录
                            distribution_data = {}
                            
                            if industry_code and re.match(r'^[A-S]', industry_code):
                                distribution_data['ttm_pe_min'] = float(pe_ttm_stats[industry_code]['min'])
                                distribution_data['ttm_pe_mean'] = float(pe_ttm_stats[industry_code]['mean'])
                                distribution_data['ttm_pe_std'] = float(pe_ttm_stats[industry_code]['std'])
                                distribution_data['ttm_pe_median'] = float(pe_ttm_stats[industry_code]['median'])
                                distribution_data['ttm_pe_percentiles'] = json.dumps(pe_ttm_stats[industry_code]['percentiles'])
                                distribution_data['ttm_pe_is_normal'] = pe_ttm_stats[industry_code]['is_normal']
                                distribution_data['pb_ratio_min'] = float(pb_stats[industry_code]['min'])
                                distribution_data['pb_ratio_mean'] = float(pb_stats[industry_code]['mean'])
                                distribution_data['pb_ratio_std'] = float(pb_stats[industry_code]['std'])
                                distribution_data['pb_ratio_median'] = float(pb_stats[industry_code]['median'])
                                distribution_data['pb_ratio_percentiles'] = json.dumps(pb_stats[industry_code]['percentiles'])
                                distribution_data['pb_ratio_is_normal'] = pb_stats[industry_code]['is_normal']
                                # print(f"distribution_data: {distribution_data}")

                                
                            valuation, created = IndustryValuation.objects.get_or_create(
                                date=latest_date,
                                industry_code=industry_code,
                                defaults={
                                    'industry_name': record['industry_name'],
                                    'parent_industry': record['parent_industry'],
                                    'stock_count': record['stock_count'],
                                    'static_pe': record['value'],
                                    **distribution_data
                                }
                            )
                            
                            # 更新其他指标
                            if not created:
                                valuation.static_pe = record['value']
                                valuation.stock_count = record['stock_count']

                                
                            # 更新其他估值指标
                            ttm_pe = next((r['value'] for r in ttm_pe_data if r['industry_code'] == industry_code), None)
                            pb_ratio = next((r['value'] for r in pb_data if r['industry_code'] == industry_code), None)
                            dividend_ratio = next((r['value'] for r in dividend_data if r['industry_code'] == industry_code), None)
                            
                            valuation.ttm_pe = ttm_pe
                            valuation.pb_ratio = pb_ratio
                            valuation.dividend_ratio = dividend_ratio
                            # valuation.ttm_pe_min = distribution_data['ttm_pe_min']
                            # valuation.pb_ratio_min = distribution_data['pb_ratio_min']

                            valuation.save()
                    
                    # 获取东方财富A股数据（前复权）
                    stock_zh_a_spot_em = ak_with_fallback(ak.stock_zh_a_spot_em, aktools_func=_aktools_stock_zh_a_spot_em)
                    logger.info(f"Fetcher: 从东方财富获取A股数据，总数量: {len(stock_zh_a_spot_em)}")

                    # 创建股票代码到数据的映射
                    em_data_map = {
                        row['代码']: {
                            'open': MarketDataFetcher.safe_float(row['今开']) or 0.0,
                            'high': MarketDataFetcher.safe_float(row['最高']) or 0.0,
                            'low': MarketDataFetcher.safe_float(row['最低']) or 0.0,
                            'close': MarketDataFetcher.safe_float(row['最新价']) or 0.0,
                            'volume': MarketDataFetcher.safe_int(row['成交量']) or 0,
                            'amount': MarketDataFetcher.safe_float(row['成交额']) or 0.0,
                            'turnover': MarketDataFetcher.safe_float(row['换手率']) or 0.0,
                            # 'pe_ttm_ratio': MarketDataFetcher.safe_float(row['市盈率-动态']),
                            # 'pb_ratio': MarketDataFetcher.safe_float(row['市净率']),
                            'total_market_value': MarketDataFetcher.safe_float(row['总市值']),
                            'float_market_value': MarketDataFetcher.safe_float(row['流通市值']),
                            'sixty_day_increase': MarketDataFetcher.safe_float(row['60日涨跌幅']),
                            'year_to_date_increase': MarketDataFetcher.safe_float(row['年初至今涨跌幅'])
                        } for _, row in stock_zh_a_spot_em.iterrows()
                    }

                    # 更新 record 数据
                    for record in stock_data:
                        code = record['code']
                        if code in em_data_map:
                            record.update(em_data_map[code])
                        
                        # 更新父级行业估值中位数
                        if record['parent_industry_code']:
                            record['parent_industry_pb_ratio_median'] = pb_stats[record['parent_industry_code']]['median']
                            record['parent_industry_pe_ttm_ratio_median'] = pe_ttm_stats[record['parent_industry_code']]['median']

                    # 先清空数据库数据
                    with transaction.atomic():
                        StockData.objects.all().delete()
                        logger.info("已清空股票历史数据")

                    # 然后再创建或更新数据库记录
                    with transaction.atomic():
                        for record in stock_data:
                            stock, created = StockData.objects.get_or_create(
                                date=latest_date,
                                code=record['code'],
                                defaults=record
                            )
                            
                            if not created:
                                for field, value in record.items():
                                    setattr(stock, field, value)
                                stock.save()
    
                    # 清理临时文件
                    os.remove(xls_path)
                    return True
            else:
                return {
                    'status': 'No need to update',
                    'message': 'Industry valuation data is up to date'
                }
            
        except Exception as e:
            print(f"Error fetching industry valuation: {str(e)}")
            return None

    @staticmethod
    def calculate_normal_distribution(data, field_name):
        """计算某个字段的正态分布情况
        参数：
        data: 数据列表
        field_name: 需要计算的列名称
        """

        # 转换为 DataFrame
        df = pd.DataFrame(data)
        
        # 按行业分组计算
        industry_stats = {}
        for industry in df['industry_code'].unique():
            industry_data = df[df['industry_code'] == industry][field_name].dropna()
            min = industry_data.min()
            
            if len(industry_data) >= 8:  # 只有样本量大于等于8时才进行正态性检验
                mean = industry_data.mean()
                std = industry_data.std()
                # 计算中位数（50%概率）
                median = industry_data.median()
                
                # 计算正态分布的分位数
                percentiles = {
                    '25%': np.percentile(industry_data, 25),
                    '50%': median,
                    '75%': np.percentile(industry_data, 75)
                }

                # 进行正态性检验
                _, p_value = stats.normaltest(industry_data)
                is_normal = p_value > 0.05
            else:
                mean = std = median = np.nan
                percentiles = {'25%': np.nan, '50%': np.nan, '75%': np.nan}
                is_normal = False
            
            industry_stats[industry] = {
                'industry_code': industry,
                'min': min,
                'mean': mean,
                'std': std,
                'median': median,
                'percentiles': percentiles,
                'is_normal': is_normal,
            }
        
        return industry_stats

    @staticmethod
    def bigdata_score(data, pe_ttm_ratio, pb_ratio):
        """计算大数投资策略评分
        参数：
        data: 行业历史估值数据
        pe_ttm_ratio: 滚动市盈率
        pb_ratio: 市净率
        公式：当前估值小于中位数(median)估值时才计算评分
        (PEx -PEmin) / (PEmedian - PEmin) = (P95% - Px) / (P95% - P50%)
        PE_score = 2 * Px -1

        (PBx -PBmin) / (PBmedian - PBmin) = (P95% - Px) / (P95% - P50%)
        PB_score = 2 * Px -1

        bigdata_score = (PE_score + PB_score) / 2
        
        返回：
        bigdata_score: 大数评分
        """

        ttm_pe_min = data.get('ttm_pe_min')
        pb_ratio_min = data.get('pb_ratio_min')
        ttm_pe_median = data.get('ttm_pe_median')
        pb_ratio_median = data.get('pb_ratio_median')
        
        # 检查所有值是否都存在
        if any(v is None for v in [ttm_pe_min, pb_ratio_min, ttm_pe_median, pb_ratio_median]):
            return 0
        
        ttm_pe_min = min(ttm_pe_min, 10)
        ttm_pe_median = min(ttm_pe_median, 25)
        pb_ratio_min = min(pb_ratio_min, 1)
        pb_ratio_median = min(pb_ratio_median, 2.5)
        
        if pe_ttm_ratio < ttm_pe_median and pb_ratio < pb_ratio_median:
            try:
                # (PEx -PEmin) / (PEmedian - PEmin) = (P95% - Px) / (P95% - P50%)
                pe_px = 0.95 - (pe_ttm_ratio - ttm_pe_min) * 0.45 / (ttm_pe_median - ttm_pe_min)
                pe_px = min(pe_px, 1.0)
                pe_score = 2 * pe_px - 1

                pb_px = 0.95 - (pb_ratio - pb_ratio_min) * 0.45 / (pb_ratio_median - pb_ratio_min)
                pb_px = min(pb_px, 1.0)
                pb_score = 2 * pb_px - 1
                
                bigdata_score = min((pe_score + pb_score) / 2, 1.0)

                return bigdata_score
            
            except Exception as e:
                return 0
        else:
            return 0

    @staticmethod
    def fetch_and_save_stock_history_data(stock, start_date, end_date, date_format='%Y%m%d'):
        """获取股票历史数据并保存到数据库
        
        Args:
            stock: StockData 对象
            start_date: 开始日期 (datetime 对象)
            end_date: 结束日期 (datetime 对象)
            date_format: 日期格式字符串，默认为 '%Y%m%d'
            
        Returns:
            hist_df: pandas DataFrame，包含历史数据，如果获取失败返回 None
        """
        try:
            # 构建请求参数
            params = {
                'symbol': str(stock.code),
                'period': 'monthly',
                'start_date': start_date.strftime(date_format),
                'end_date': end_date.strftime(date_format),
                'adjust': 'qfq'
            }
            
            # 发送请求到 AKTools API
            # 从环境变量中获取 AKTOOLS_URL
            aktools_url = os.environ.get('AKTOOLS_URL')
            
            try:
                response = requests.get(
                    f'{aktools_url}/api/public/stock_zh_a_hist',
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                # 解析 JSON 响应
                data = response.json()
                hist_df = pd.DataFrame(data)
                time.sleep(5)
                
            except Exception as api_error:
                # 如果 AKTools API 失败，回退到原始的 akshare 方法
                logger.warning(f"AKTools API 请求失败，回退到 akshare: {str(api_error)}")
                hist_df = ak.stock_zh_a_hist(
                    symbol=str(stock.code),
                    period="monthly",
                    start_date=start_date.strftime(date_format),
                    end_date=end_date.strftime(date_format),
                    adjust="qfq"
                )
                time.sleep(5)
            
            if not hist_df.empty:
                # 保存新数据到本地
                for _, row in hist_df.iterrows():
                    # 安全处理日期：如果已经是 date 对象就直接使用，否则转换为 date
                    row_date = row['日期']
                    if isinstance(row_date, datetime):
                        row_date = row_date.date()
                    elif not isinstance(row_date, date):
                        # 如果是字符串或其他类型，尝试转换
                        row_date = pd.to_datetime(row_date).date()
                    
                    StockHistoryData.objects.update_or_create(
                        date=row_date,
                        code=stock.code,
                        period="monthly",
                        adjust="qfq",
                        defaults={
                            'name': stock.name,
                            'close': row['收盘']
                        }
                    )
                
                logger.info(f"成功保存股票 {stock.code} 的历史数据，共 {len(hist_df)} 条记录")
                return hist_df
            else:
                logger.warning(f"股票 {stock.code} 的历史数据为空")
                return None
                
        except Exception as e:
            logger.error(f"获取股票 {stock.code} 历史数据时出错: {str(e)}")
            return None

    @staticmethod
    def fetch_bigdata_strategy_data():
        """获取大数投资策略数据
        
        筛选条件：
        1. 滚动 PE < 20
        2. PB < 2
        3. 总市值 > 3亿
        4. 股价 > 2元
        5. 60日涨幅 < 50%
        6. 非ST股票
        7. 近一年涨幅 < 100%
        """
        try:
            # 先检查空值情况（CSI 获取的数据中静态市盈率、滚动市盈率、市净率可能为空）
            null_stats = {
                'pe_ttm_ratio': StockData.objects.filter(pe_ttm_ratio__isnull=True).count(),
                'pb_ratio': StockData.objects.filter(pb_ratio__isnull=True).count(),
                'total_market_value': StockData.objects.filter(total_market_value__isnull=True).count(),
                'close': StockData.objects.filter(close__isnull=True).count(),
                'sixty_day_increase': StockData.objects.filter(sixty_day_increase__isnull=True).count(),
            }
            logger.info(f"Fetcher: 股票数据空值统计: {null_stats}")

            # 批量筛选符合条件的股票(数据库只有最近一个交易日的数据)
            stocks = StockData.objects.filter(
                pe_ttm_ratio__lt=20,
                pb_ratio__lt=2,
                total_market_value__gt=300000000,
                close__gt=2,
                sixty_day_increase__lt=50,
                year_to_date_increase__lt=100
            ).exclude(
                name__iregex=r'.*st.*'
            )
            logger.info(f"Fetcher: 符合大数投资策略的股票数量: {len(stocks)}")

            # 使用企业所属行业的估值中位数作为筛选条件，以便适应个行业的不同特性
            stocks_industry_valuation = StockData.objects.filter(
                pe_ttm_ratio__lt=F('parent_industry_pe_ttm_ratio_median'),
                pb_ratio__lt=F('parent_industry_pb_ratio_median'),
                total_market_value__gt=300000000,
                close__gt=2,
                sixty_day_increase__lt=50,
                year_to_date_increase__lt=100
            ).exclude(
                name__iregex=r'.*st.*'
            )

            

            # 大数评分：查询行业历史估值数据，为计算大数评分做准备
            industry_history_data = IndustryValuation.objects.filter(
                industry_code__regex=r'^[A-S]',
                date=stocks[0].date # 取最近的数据即可，历史数据计算已经在抓取时完成
            ).values('industry_code', 'ttm_pe_min', 'pb_ratio_min', 'ttm_pe_median', 'pb_ratio_median')
            
            # 大数评分：转换为字典
            industry_stats = {
                item['industry_code']: item 
                for item in industry_history_data
            }

            # 准备批量更新的数据
            bulk_data = []

            # 测试阶段：只处理前3个数据，每个间隔5秒
            # 测试完成后，如需恢复生产模式，将 stocks[:3] 改回 stocks，并将间隔逻辑改回每10个暂停1秒
            # test_stocks = stocks[:25]
            # logger.info(f"测试模式：只处理前 {len(test_stocks)} 个股票，每个间隔5秒")

            # 准备日期范围（计算近一年股票涨幅）
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            date_format = '%Y%m%d'
            
            for i, stock in enumerate(stocks):
                logger.info(f"处理股票: {stock.code} - {stock.name}")

                try:
                    # 每个股票处理间隔5秒
                    if i > 0:
                        logger.info(f"等待5秒后处理下一个股票... (当前: {i}/{len(stocks)})")
                    
                    # 获取历史数据
                    try:
                        # 先检查本地是否有该股票的历史数据
                        
                        existing_data = StockHistoryData.objects.filter(
                            code=stock.code,
                            period="monthly",
                            adjust="qfq"
                        ).order_by('-date')
                        
                        if existing_data.exists():
                            logger.info(f"有本地数据，从StockData中获取最新close数据")
                            # StockHistoryData中有数据，则从StockData中获取code对应的最新close数据
                            # 如果close数据和本地最新数据是同一个月，则更新本地数据
                            # 如果close数据和本地最新数据不是同一个月，则在StockHistoryData中新增一条数据
                            
                            # 获取 StockHistoryData 中的最新数据
                            latest_history = existing_data.first()
                            latest_history_date = latest_history.date
                            
                            # stock 对象已经包含了最新的 StockData 数据（因为查询时只返回最近一个交易日的数据）
                            stock_date = stock.date
                            
                            # 比较两个日期的月份和年份
                            if (stock_date.year == latest_history_date.year and 
                                stock_date.month == latest_history_date.month):
                                # 如果是同一个月，更新 StockHistoryData 中的 close 数据
                                logger.info(f"StockData 和 StockHistoryData 最新数据是同一个月 ({stock_date.strftime('%Y-%m')})，更新 close 数据: {stock.close}")
                                latest_history.close = stock.close
                                latest_history.save()
                            else:
                                # 如果不是同一个月，在 StockHistoryData 中新增一条数据
                                logger.info(f"StockData 和 StockHistoryData 最新数据不是同一个月 (StockData: {stock_date.strftime('%Y-%m')}, History: {latest_history_date.strftime('%Y-%m')})，新增一条数据")
                                StockHistoryData.objects.update_or_create(
                                    date=stock_date,
                                    code=stock.code,
                                    period="monthly",
                                    adjust="qfq",
                                    defaults={
                                        'name': stock.name,
                                        'close': stock.close
                                    }
                                )
                            
                            # 重新获取本地数据用于计算
                            local_data = StockHistoryData.objects.filter(
                                code=stock.code,
                                period="monthly",
                                adjust="qfq"
                            ).order_by('date')
                            
                            # 计算近一年最低点涨幅
                            one_year_min = local_data.aggregate(min_close=Min('close'))['min_close']
                            one_year_increase = (stock.close - one_year_min) / one_year_min * 100 if one_year_min else None
                        else:
                            # 没有本地数据，从网络获取并保存
                            logger.info(f"没有本地数据，获取最新数据")
                            hist_df = MarketDataFetcher.fetch_and_save_stock_history_data(
                                stock, start_date, end_date, date_format
                            )
                                
                            if hist_df is not None and not hist_df.empty:
                                # 计算近一年最低点涨幅
                                one_year_min = hist_df['收盘'].min()
                                one_year_increase = (stock.close - one_year_min) / one_year_min * 100
                            else:
                                one_year_min = one_year_increase = None
                    except Exception as e:
                        # 如果获取历史数据失败，使用默认值
                        logger.warning(f"获取股票 {stock.code} 历史数据失败，使用默认值: {str(e)}")
                        one_year_min = one_year_increase = None
                    
                    # 大数评分：获取具体行业历史估值数据
                    stats = industry_stats.get(stock.parent_industry_code, {})

                    # 大数评分：计算评分
                    bigdata_score = MarketDataFetcher.bigdata_score(
                        stats,
                        stock.pe_ttm_ratio, 
                        stock.pb_ratio
                    )

                    # 添加到批量更新列表
                    bulk_data.append(
                        BigDataStrategyStockData(
                            date=stock.date,
                            code=stock.code,
                            name=stock.name,
                            close=stock.close,
                            pe_ratio=stock.pe_ratio,
                            pe_ttm_ratio=stock.pe_ttm_ratio,
                            pb_ratio=stock.pb_ratio,
                            dividend_ratio=stock.dividend_ratio,
                            industry_name=stock.industry_name,
                            industry_code=stock.industry_code,
                            parent_industry_code=stock.parent_industry_code,
                            total_market_value=stock.total_market_value,
                            sixty_day_increase=stock.sixty_day_increase,
                            year_to_date_increase=stock.year_to_date_increase,
                            one_year_min=one_year_min,
                            one_year_increase=round(one_year_increase, 2),
                            bigdata_score=round(bigdata_score * 100, 2),
                            bigdata_score_method='行业估值'
                        )
                    )
                    
                except Exception as e:
                    logging.error(f"处理股票 {stock.code} 历史数据时出错: {str(e)}")
                    continue
            
            # 批量更新数据库
            if bulk_data:
                with transaction.atomic():
                    # 先删除所有的数据
                    BigDataStrategyStockData.objects.all().delete()
                    # 批量创建新数据
                    BigDataStrategyStockData.objects.bulk_create(
                        bulk_data,
                        batch_size=100  # 每批处理100条记录
                    )
            
            return {
                'status': 'success',
                'message': f'成功更新 {len(bulk_data)} 只股票的数据'
            }
            
        except Exception as e:
            logging.error(f"获取大数投资策略数据时出错: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }

    @staticmethod
    def fetch_index_data(start_date=None, end_date=None):
        """获取指数历史数据"""
        try:
            # 从数据库获取所有指数
            main_indices = IndexData.objects.values_list('code', flat=True).distinct().order_by('code')

            for symbol in main_indices:
                try:
                    df = ak.stock_zh_index_hist_csindex(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if df.empty:
                        print(f"No data fetched for symbol {symbol}")
                        continue  # 跳过空数据，继续处理下一个指数
                    
                    # 数据处理和转换
                    numeric_columns = ['开盘', '最高', '最低', '收盘', '成交量', '成交金额', '滚动市盈率']
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df['日期'] = pd.to_datetime(df['日期'])
                    df = df.dropna(subset=['收盘', '成交量', '成交金额'])
                    
                    if df.empty:
                        print(f"No valid data after cleaning for symbol {symbol}")
                        continue
                    
                    current_data = df.iloc[-1]
                    
                    # 计算百分位
                    current_date = current_data['日期'].date()
                    twelve_years_ago = current_date - pd.DateOffset(years=12)
                    historical_data = list(IndexData.objects.filter(
                        code=symbol, 
                        date__gte=twelve_years_ago
                    ).values_list('close', flat=True))
                    
                    percentile = None
                    if historical_data:
                        current_close = float(current_data['收盘'])
                        percentile = len([x for x in historical_data if x <= current_close]) / len(historical_data) * 100
                        percentile = round(percentile, 2)

                    # 创建记录
                    index_data = IndexData(
                        date=current_data['日期'].date(),
                        code=symbol,
                        name=str(current_data['指数中文全称']), 
                        open=float(current_data['开盘']) if pd.notna(current_data['开盘']) else None,
                        high=float(current_data['最高']) if pd.notna(current_data['最高']) else None,
                        low=float(current_data['最低']) if pd.notna(current_data['最低']) else None,
                        close=float(current_data['收盘']),
                        volume=int(current_data['成交量']) if pd.notna(current_data['成交量']) else None,
                        amount=float(current_data['成交金额']) if pd.notna(current_data['成交金额']) else None,
                        pe_ratio=None,
                        pe_ttm_ratio=float(current_data['滚动市盈率']) if pd.notna(current_data.get('滚动市盈率')) else None,
                        pb_ratio=None,
                        percentile=percentile
                    )

                    with transaction.atomic():
                        IndexData.objects.bulk_create(
                            [index_data],
                            update_conflicts=True,
                            unique_fields=['date', 'code'],
                            update_fields=['open', 'high', 'low', 'close', 'volume', 'amount', 'percentile']
                        )
                        
                except Exception as e:
                    print(f"Error processing symbol {symbol}: {str(e)}")
                    continue

            return {
                'status': 'success',
                'message': 'Index data updated successfully'
            }
                
        except Exception as e:
            return {
                'status': 'error',
                'message': f"Error fetching index data: {str(e)}"
            }

    

    ## 指数数据采集（批量增加）

    @staticmethod
    def fetch_index_data_batch(symbol, start_date=None, end_date=None):
        """获取指数历史数据
        
        Args:
            symbol: 指数代码，如 '930903'
            start_date: 开始日期，格式 'YYYYMMDD'
            end_date: 结束日期，格式 'YYYYMMDD'
            中证A股指数 (930903): https://www.csindex.com.cn/zh-CN/indices/index-detail/H30374#/indices/family/detail?indexCode=930903
        """
        try:
            # 获取指数数据
            df = ak.stock_zh_index_hist_csindex(
                symbol=symbol,
                start_date='20000101',
                end_date=end_date
            )
            
            if df.empty:
                print("No data fetched")
                return False
                
            # 转换数据类型
            df['开盘'] = pd.to_numeric(df['开盘'], errors='coerce')
            df['最高'] = pd.to_numeric(df['最高'], errors='coerce')
            df['最低'] = pd.to_numeric(df['最低'], errors='coerce')
            df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
            df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
            df['成交金额'] = pd.to_numeric(df['成交金额'], errors='coerce')
            
            # 过滤掉无效数据
            df = df.dropna(subset=['收盘', '成交量', '成交金额'])
            
            if df.empty:
                print("No valid data after cleaning")
                return False
            
            # 转换日期为 datetime 类型
            df['日期'] = pd.to_datetime(df['日期'])
            
            # 创建记录
            index_data_list = []
            df_start = df[df['日期'] >= start_date]
            for _, row in df_start.iterrows():
                # 计算近12年的百分位
                current_date = row['日期'].date()
                twelve_years_ago = current_date - pd.DateOffset(years=12)
                latest_12_years_data = df[(pd.to_datetime(current_date) >= df['日期']) & (df['日期'] >= twelve_years_ago)]
                

                # 计算收盘价的百分位
                if not latest_12_years_data.empty:
                    close_series = latest_12_years_data['收盘']
                    current_close = float(row['收盘'])
                    percentile = len(close_series[close_series <= current_close]) / len(close_series) * 100
                    percentile = round(percentile, 2)
                else:
                    percentile = None

                try:
                    index_data = IndexData(
                        date=row['日期'].date(),
                        code=symbol,
                        name=str(row['指数中文全称']),
                        open=float(row['开盘']) if pd.notna(row['开盘']) else None,
                        high=float(row['最高']) if pd.notna(row['最高']) else None,
                        low=float(row['最低']) if pd.notna(row['最低']) else None,
                        close=float(row['收盘']),
                        volume=int(row['成交量']),
                        amount=float(row['成交金额']),
                        pe_ratio=None,
                        pe_ttm_ratio=float(row['滚动市盈率']),
                        pb_ratio=None,
                        percentile=percentile
                    )
                    index_data_list.append(index_data)
                except (ValueError, TypeError) as e:
                    print(f"Error processing row: {e}")
                    continue
                
            if not index_data_list:
                print("No valid records to insert")
                return False
                
            # 使用 bulk_create 批量创建记录
            with transaction.atomic():
                IndexData.objects.bulk_create(
                    index_data_list,
                    update_conflicts=True,
                    unique_fields=['date', 'code'],
                    update_fields=['open', 'high', 'low', 'close', 'volume', 'amount', 'pe_ttm_ratio', 'percentile']
                )
                
            return True
                
        except Exception as e:
            print(f"Error fetching index data: {str(e)}")
            return False
        
class ConvertibleBondMarketDataFetcher:
    """可转债市场数据采集"""
    
    @staticmethod
    def fetch_bond_index_data():
        """获取可转债指数数据"""
        logger = logging.getLogger(__name__)

        try:
            # 获取可转债指数数据
            bond_index_df = ak.bond_cb_index_jsl()
            logger.info(f"获取到可转债指数数据")
            
            if bond_index_df.empty:
                return {
                    'status': 'error',
                    'message': 'No bond index data fetched'
                }
            
            # 打印数据框的列名，帮助调试
            logger.debug(f"可转债指数数据列名: {bond_index_df.columns.tolist()}")
            
            def convert_percentage(value):
                """转换百分比值"""
                try:
                    if isinstance(value, str):
                        return float(value.strip('%'))
                    return float(value)
                except (ValueError, AttributeError) as e:
                    logger.error(f"转换百分比值失败: {value}, 错误: {str(e)}")
                    return 0.0

            try:
                # 从数据框获取最后一行数据
                row = bond_index_df.iloc[-1]
                
                # 构建数据字典
                index_data = {
                    'date': row['price_dt'],
                    'code': 'CB_INDEX',  # 可转债指数的代码
                    'name': '可转债指数',  # 指数名称
                    'price': float(row['price']),  # 指数
                    'increase_rt': convert_percentage(row['increase_rt']),  # 涨幅
                    'avg_price': float(row['avg_price']),  # 平均价格
                    'mid_price': float(row['mid_price']),  # 中位数价格
                    'avg_premium_rt': convert_percentage(row['avg_premium_rt']),  # 平均溢价率
                    'mid_premium_rt': convert_percentage(row['mid_premium_rt']),  # 中位数溢价率
                    'avg_ytm_rt': convert_percentage(row['avg_ytm_rt']),  # 平均收益率
                    
                    # 价格分布统计
                    'price_90': int(row['price_90']),  # >90
                    'price_90_100': int(row['price_90_100']),  # 90~100
                    'price_100_110': int(row['price_100_110']),  # 100~110
                    'price_110_120': int(row['price_110_120']),  # 110~120
                    'price_120_130': int(row['price_120_130']),  # 120~130
                    'price_130': int(row['price_130']),  # >130
                }

                # 更新数据库
                BondIndexData.objects.update_or_create(
                    date=row['price_dt'],
                    code='CB_INDEX',
                    defaults=index_data
                )

                return {
                    'status': 'success',
                    'message': 'Successfully updated bond index data'
                }

            except Exception as e:
                logger.error(f"处理可转债指数数据时出错: {str(e)}", exc_info=True)
                return {
                    'status': 'error',
                    'message': f"Error processing bond index data: {str(e)}"
                }

        except Exception as e:
            logger.error(f"获取可转债指数数据时出错: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f"Error fetching bond index data: {str(e)}"
            }

    @staticmethod
    def fetch_convertible_bond_market_data():
        """获取可转债市场数据（集思录）"""
        jisilu_api_cookie = settings.JISILU_API_COOKIE
        logger = logging.getLogger(__name__)

        date = datetime.now().date()

        try:
            # 获取可转债数据
            bond_cb_df = ak.bond_cb_jsl(cookie=jisilu_api_cookie)
            
            # 获取可转债强赎数据
            bond_redeem_df = ak.bond_cb_redeem_jsl()

            # 获取可转债再次强赎数据
            bond_next_redeem_df = ConvertibleBondMarketDataFetcher.fetch_convertible_bond_next_redeem_data()
            
            if not bond_next_redeem_df.empty:
                bond_next_redeem_df = pd.DataFrame(bond_next_redeem_df)
                # 筛选出 bond_next_redeem_df 中，强赎天计数包含 “不强赎” 的记录
                bond_next_redeem_df = bond_next_redeem_df[bond_next_redeem_df['强赎天计数'].str.contains('不强赎')]
    
                # 设置索引为“转债代码”以便更新
                bond_redeem_df.set_index('代码', inplace=True)
                bond_next_redeem_df.set_index('转债代码', inplace=True)
                
                # 更新 bond_redeem_df 中的相关列
                bond_redeem_df.update(bond_next_redeem_df)
                
                # 重置索引
                bond_redeem_df.reset_index(inplace=True)


            bond_data_dict = {}
            
            for _, row in bond_cb_df.iterrows():
                try:
                    bond_id = row.get('代码', 'unknown')
                    
                    # 处理日期字段
                    def parse_date(date_str):
                        try:
                            if pd.isna(date_str):
                                return None
                            return pd.to_datetime(date_str).date()
                        except:
                            return None

                    # 从主数据获取基本信息
                    bond_data = {
                        'date': date,
                        'code': str(row['代码']),
                        'name': str(row['转债名称']),
                        'close': float(row['现价']),
                        'stock_code': str(row['正股代码']),
                        'stock_name': str(row['正股名称']),
                        'stock_price': float(row['正股价']),
                        'stock_pb': float(row['正股PB']),
                        'convertible_price': float(row['转股价']),
                        'convertible_value': float(row['转股价值']),
                        'premium_rate': float(str(row['转股溢价率']).strip('%')),
                        'bond_rating': str(row.get('债券评级', '')),
                        'remaining_size': round(float(row['剩余规模']), 2),
                        'ytm_before_tax': float(str(row.get('到期税前收益', '0')).strip('%')),
                        'maturity_date': parse_date(row['到期时间']),
                        'double_low': float(row['双低'])
                    }
                    
                    def clean_text(text):
                        """清理文本中的特殊字符和HTML标签"""
                        if not isinstance(text, str):
                            return str(text)
                        # 移除HTML标签和特殊字符
                        text = re.sub(r'<[^>]+>', '', text)  # 移除HTML标签
                        text = re.sub(r'[!！]', '', text)    # 移除感叹号
                        return text.strip()

                    # 从强赎数据获取强赎相关信息
                    if not bond_redeem_df.empty:
                        matching_records = bond_redeem_df[bond_redeem_df['代码'] == bond_data['code']]
                        if not matching_records.empty:
                            redeem_info = matching_records.iloc[0]
                            bond_data.update({
                                'is_callable': redeem_info.get('强赎状态', ''),
                                'redemption_trigger_price': float(redeem_info.get('强赎触发价', 0)),
                                'redemption_price': float(redeem_info.get('强赎价', 0)),
                                'redemption_countdown': clean_text(redeem_info.get('强赎天计数', '')),
                                'last_trading_date': parse_date(redeem_info.get('最后交易日')),
                                'convertible_start_date': parse_date(redeem_info.get('转股起始日'))
                            })
                    
                    bond_data_dict[bond_data['code']] = bond_data
                    
                except Exception as e:
                    logger.error(f"处理转债 {bond_id} 时出错: {str(e)}", exc_info=True)
                    continue

            # 批量更新数据库
            success_count = 0
            with transaction.atomic():
                for code, bond_data in bond_data_dict.items():
                    try:
                        BondData.objects.update_or_create(
                            code=code,
                            defaults=bond_data,
                        )
                        success_count += 1
                    except Exception as e:
                        logger.error(f"更新转债 {code} 数据时出错: {str(e)}", exc_info=True)

            return {
                'status': 'success',
                'message': f'Successfully updated {success_count} out of {len(bond_data_dict)} convertible bonds'
            }

        except Exception as e:
            logger.error(f"获取可转债数据时出错: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f"Error fetching convertible bond data: {str(e)}"
            }
        
    @staticmethod
    def fetch_convertible_bond_market_data_dongfangcaifu():
        """获取可转债市场数据（东方财富+集思录）"""

        logger = logging.getLogger(__name__)
        date = datetime.now().date()

        try:
            # 获取可转债数据（东方财富）
            bond_cb_df = ak.bond_zh_cov()

            # 获取可转债强赎数据（集思录）
            bond_redeem_df = ak.bond_cb_redeem_jsl()

            # 获取可转债再次强赎数据
            bond_next_redeem_df = ConvertibleBondMarketDataFetcher.fetch_convertible_bond_next_redeem_data()
            bond_next_redeem_df = pd.DataFrame(bond_next_redeem_df)
            
            if not bond_next_redeem_df.empty:
                # 筛选出 bond_next_redeem_df 中，强赎天计数包含 “不强赎” 的记录
                bond_next_redeem_df = bond_next_redeem_df[bond_next_redeem_df['强赎天计数'].str.contains('不强赎')]
    
                # 设置索引为“转债代码”以便更新
                bond_redeem_df.set_index('代码', inplace=True)
                bond_next_redeem_df.set_index('转债代码', inplace=True)
                
                # 更新 bond_redeem_df 中的相关列
                bond_redeem_df.update(bond_next_redeem_df)
                
                # 重置索引
                bond_redeem_df.reset_index(inplace=True)

            # 获取正股数据 (数据库查询)
            stock_code_list = bond_cb_df['正股代码'].unique().tolist()
            stock_data = StockData.objects.filter(
                # date=date, 
                code__in=stock_code_list
            ).values(
                'code', 
                'pb_ratio', 
                'pe_ttm_ratio', 
                'parent_industry_pb_ratio_median', 
                'parent_industry_pe_ttm_ratio_median'
            )
            
            # 获取已手动标记风险的数据
            risk_excluded_data = BondData.objects.filter(is_risk_excluded=True).values('code')
            risk_excluded_code_list = [item['code'] for item in risk_excluded_data]
            logger.info(f"已手动标记风险 code list: {risk_excluded_code_list}")

            bond_data_dict = {}
            
            for _, row in bond_cb_df.iterrows():
                try:
                    bond_id = row.get('债券代码', 'unknown')
                    
                    # 处理日期字段
                    def parse_date(date_str):
                        try:
                            if pd.isna(date_str):
                                return None
                            return pd.to_datetime(date_str).date()
                        except:
                            return None

                    # 从主数据获取基本信息
                    bond_data = {
                        'date': date,
                        'code': str(row['债券代码']),
                        'name': str(row['债券简称']),
                        'subscription_date': parse_date(row.get('申购日期')),
                        'subscription_code': str(row.get('申购代码', '')),
                        'subscription_record_date': parse_date(row.get('原股东配售-股权登记日')),
                        'subscription_per_share': float(row.get('原股东配售-每股配售额', 0)),
                        'subscription_announcement_date': parse_date(row.get('中签号发布日')),
                        'subscription_rate': float(str(row.get('中签率', '0')).strip('%')),
                        'listing_date': parse_date(row.get('上市时间')),
                        'close': float(row['债现价']),
                        'stock_code': str(row['正股代码']),
                        'stock_name': str(row['正股简称']),
                        'stock_price': float(row['正股价']),
                        'convertible_price': float(row['转股价']),
                        'convertible_value': float(row['转股价值']),
                        'premium_rate': float(str(row['转股溢价率']).strip('%')),
                        'bond_rating': str(row.get('信用评级', '')),
                        'issue_size': float(row.get('发行规模', 0)),  # 单位: 亿元
                    }
                    
                    # 计算双低值
                    bond_data['double_low'] = bond_data['close'] + bond_data['premium_rate']
                    
                    # 获取正股数据
                    stock_data_dict = {stock['code']: stock for stock in stock_data}
                    try:
                        if bond_data['stock_code'] in stock_data_dict:
                            bond_data['stock_pb'] = stock_data_dict[bond_data['stock_code']]['pb_ratio']
                            bond_data['stock_pe_ttm_ratio'] = stock_data_dict[bond_data['stock_code']]['pe_ttm_ratio']
                            bond_data['stock_industry_pb_ratio_median'] = stock_data_dict[bond_data['stock_code']]['parent_industry_pb_ratio_median']
                            bond_data['stock_industry_pe_ttm_ratio_median'] = stock_data_dict[bond_data['stock_code']]['parent_industry_pe_ttm_ratio_median']
                        else:
                            # 设置默认值
                            bond_data['stock_pb'] = None
                            bond_data['stock_pe_ttm_ratio'] = None
                            bond_data['stock_industry_pb_ratio_median'] = None
                            bond_data['stock_industry_pe_ttm_ratio_median'] = None
                    except Exception as e:
                        logger.error(f"处理正股数据时出错: {str(e)}")
                        # 设置默认值
                        bond_data['stock_pb'] = None
                        bond_data['stock_pe_ttm_ratio'] = None
                        bond_data['stock_industry_pb_ratio_median'] = None
                        bond_data['stock_industry_pe_ttm_ratio_median'] = None
                    
                    # 清理强赎字段中的特殊字符和HTML标签
                    def clean_text(text):
                        """清理文本中的特殊字符和HTML标签"""
                        if not isinstance(text, str):
                            return str(text)
                        # 移除HTML标签和特殊字符
                        text = re.sub(r'<[^>]+>', '', text)  # 移除HTML标签
                        text = re.sub(r'[!！]', '', text)    # 移除感叹号
                        return text.strip()

                    # 从强赎数据获取强赎相关信息
                    if not bond_redeem_df.empty:
                        matching_records = bond_redeem_df[bond_redeem_df['代码'] == bond_data['code']]
                        if not matching_records.empty:
                            redeem_info = matching_records.iloc[0]
                            bond_data.update({
                                'is_callable': redeem_info.get('强赎状态', ''),
                                'redemption_trigger_price': float(redeem_info.get('强赎触发价', 0)),
                                'redemption_price': float(redeem_info.get('强赎价', 0)),
                                'redemption_countdown': clean_text(redeem_info.get('强赎天计数', '')),
                                'convertible_start_date': parse_date(redeem_info.get('转股起始日')),
                                'last_trading_date': parse_date(redeem_info.get('最后交易日')),
                                'maturity_date': parse_date(redeem_info.get('到期日')),
                                'remaining_size': float(redeem_info.get('剩余规模', 0)),  # 单位: 亿元
                            })
                    
                    # 继承手动排风险的状态
                    if bond_data['code'] in risk_excluded_code_list:
                        bond_data['is_risk_excluded'] = True
                    else:
                        bond_data['is_risk_excluded'] = False
                    
                    
                    bond_data_dict[bond_data['code']] = bond_data
                    
                except Exception as e:
                    logger.error(f"处理转债 {bond_id} 时出错: {str(e)}", exc_info=True)
                    continue
            
            # 先清空当天的数据
            with transaction.atomic():
                BondData.objects.all().delete()
                logger.info("已清空可转债历史数据")

            # 批量更新数据库
            success_count = 0
            with transaction.atomic():
                for code, bond_data in bond_data_dict.items():
                    try:
                        BondData.objects.update_or_create(
                            code=code,
                            defaults=bond_data,
                        )
                        success_count += 1
                    except Exception as e:
                        logger.error(f"更新转债 {code} 数据时出错: {str(e)}", exc_info=True)

            return {
                'status': 'success',
                'message': f'Successfully updated {success_count} out of {len(bond_data_dict)} convertible bonds'
            }

        except Exception as e:
            logger.error(f"获取可转债数据时出错: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f"Error fetching convertible bond data: {str(e)}"
            }
        
    @staticmethod
    def fetch_convertible_bond_next_redeem_data():
        """获取可转债强赎_再次强赎数据_集思录
        爬取页面 https://www.jisilu.cn/web/data/cb/redeem 的数据
        查找“强赎天计数“列中，值符合“不强赎 2024-12-13重新计”形式的记录，并更新到数据库中
        """

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # 使用无头模式
            context = browser.new_context()
            page = context.new_page()
            
            # 打开目标页面
            page.goto("https://www.jisilu.cn/web/data/cb/redeem")
            
            # 等待表格加载完成
            page.wait_for_selector(".jsl-table-body-wrapper")  # 表格的父级选择器
            
            # 提取表格行
            rows = page.query_selector_all(".jsl-table-body-wrapper tbody tr")
            data = []
            
            for row in rows:
                cells = row.query_selector_all("td")

                if not cells:  # 如果没有单元格，跳过
                    continue
            
                # 根据单元格顺序提取数据
                record = {
                    "转债代码": cells[0].get_attribute("name").strip() if cells[0].get_attribute("name") else "",  # 从 name 属性获取数据
                    # "转债现价": cells[2].inner_text().strip(),
                    # "规模": cells[5].inner_text().strip(),
                    # "剩余规模": cells[6].inner_text().strip(),
                    # "转股价": cells[8].inner_text().strip(),
                    # "强赎触发比": cells[9].inner_text().strip(),
                    # "强赎触发价": cells[10].inner_text().strip(),
                    # "正股价": cells[11].inner_text().strip(),
                    # "强赎价": cells[12].inner_text().strip(),
                    "强赎天计数": cells[13].inner_text().strip(),
                    # "强赎条款": cells[14].inner_text().strip(),
                }
                data.append(record)
        
            browser.close()
            
            return data

class RefreshDatabaseCache:
    """刷新数据库缓存"""
    @staticmethod
    def refresh_database_cache():
        """刷新数据库缓存"""
        from django.core.cache import cache
        cache.clear()
        # 如果需要，可以在这里添加其他刷新逻辑
        return True
