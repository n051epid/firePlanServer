import logging
from celery import shared_task
from django.core.mail import EmailMessage
from .market_data.fetcher import MarketDataFetcher, ConvertibleBondMarketDataFetcher
from celery.exceptions import MaxRetriesExceededError
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

@shared_task
def send_notification_email(subject, message, from_email, recipient_list, bcc_list):
    try:
        email = EmailMessage(
            subject,
            message,
            from_email,
            recipient_list,
            bcc=bcc_list
        )
        sent = email.send(fail_silently=False)
        
        if sent == 1:
            logger.info(f"Successfully sent notification email to {recipient_list[0]} and bcc to {bcc_list[0]}")
            return True
        else:
            logger.warning(f"Failed to send notification email to {recipient_list[0]}")
            return False
    except Exception as e:
        logger.error(f"Failed to send notification email: {str(e)}")
        return False



@shared_task(bind=True, max_retries=3, default_retry_delay=60)  # 最多重试3次，每次间隔60秒
def fetch_daily_market_data(self):
    """每日市场数据采集任务"""
    try:
        # 创建 fetcher 实例并直接调用
        fetcher = MarketDataFetcher()
        market_data = fetcher.fetch_daily_market_data()
        date = datetime.now().strftime('%Y-%m-%d')

        if market_data:
            print(f"成功获取市场数据 by celery: {date}")
            return {
                'status': 'success',
                'date': date,
                'message': market_data['message']
            }
        
        try:
            self.retry()
        except MaxRetriesExceededError:
            return {'status': 'error', 'message': 'Max retries exceeded'}
            
    except Exception as e:
        print(f"Error in daily market data task: {str(e)}")
        return {'status': 'error', 'message': str(e)}


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_index_data(self):
    """指数数据采集任务"""
    try:
        # 自动计算日期范围（比如获取最近10个交易日的数据）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
        
        fetcher = MarketDataFetcher()
        result = fetcher.fetch_index_data(    
            start_date=start_date,
            end_date=end_date
        )

        if result:
            print(f"指数数据获取成功")
            return {
                'status': 'success',
                'date_range': f"{start_date} - {end_date}"
            }
        
        try:
            self.retry()
        except MaxRetriesExceededError:
            return {'status': 'error', 'message': 'Max retries exceeded'}

    except Exception as e:
        print(f"Error in index_data_task: {str(e)}")
        return {'status': 'error', 'message': str(e)}
    

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_index_data_batch(self, symbol='930903', start_date=None, end_date=None):
    """指数数据采集任务（批量增加）
       python manage.py shell
       from fire_100UpPlan.tasks import fetch_index_data_batch
       result = fetch_index_data_batch('000991','20120101','20241116')
    """
    try:
        # 自动计算日期范围（比如获取最近10个交易日的数据）
        if not start_date or not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
        
        fetcher = MarketDataFetcher()
        result = fetcher.fetch_index_data_batch(    
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )

        if result:
            print(f"指数数据获取成功: {symbol}")
            return {
                'status': 'success',
                'date_range': f"{start_date} - {end_date}"
            }
        
        try:
            self.retry()
        except MaxRetriesExceededError:
            return {'status': 'error', 'message': 'Max retries exceeded'}

    except Exception as e:
        print(f"Error in index_data_task: {str(e)}")
        return {'status': 'error', 'message': str(e)}

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_margin_trading_data(self):
    """两融数据采集任务"""
    try:
        fetcher = MarketDataFetcher()
        result = fetcher.fetch_margin_trading_data()

        if result:
            print(f"两融数据获取成功")
            return {'status': 'success'}
        
        try:
            self.retry()
        except MaxRetriesExceededError:
            return {'status': 'error', 'message': 'Max retries exceeded'}
        
    except Exception as e:
        print(f"Error in margin_trading_data_task: {str(e)}")
        return {'status': 'error', 'message': str(e)}
    

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_industry_valuation_data(self, start_date=None, end_date=None):
    """行业估值数据采集任务"""
    try:
        fetcher = MarketDataFetcher()
        
        # 如果没有指定日期范围，默认获取当天的数据
        if not start_date or not end_date:
            end_date = datetime.now()
            start_date = end_date
        else:
            start_date = datetime.strptime(start_date, '%Y%m%d')
            end_date = datetime.strptime(end_date, '%Y%m%d')
        
        # 生成日期范围
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            # 跳过周末
            if current_date.weekday() < 5:  # 0-4 表示周一到周五
                date_list.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        
        success_count = 0
        fail_count = 0
        
        # 遍历日期范围获取数据
        for date in date_list:
            try:
                result = fetcher.fetch_industry_valuation(date)
                if result:
                    print(f"行业估值数据获取成功: {date}")
                    success_count += 1
                else:
                    print(f"行业估值数据获取失败: {date}")
                    fail_count += 1
            except Exception as e:
                print(f"处理日期 {date} 时出错: {str(e)}")
                fail_count += 1
        
        return {
            'status': 'success',
            'message': f'处理完成: 成功 {success_count} 天, 失败 {fail_count} 天',
            'date_range': f"{start_date.strftime('%Y%m%d')} - {end_date.strftime('%Y%m%d')}"
        }
        
    except Exception as e:
        print(f"Error in industry_valuation_data_task: {str(e)}")
        try:
            self.retry()
        except MaxRetriesExceededError:
            return {
                'status': 'error',
                'message': f'Max retries exceeded: {str(e)}'
            }
        

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_bigdata_strategy_data(self):
    """大数投资策略数据采集任务"""
    try:
        fetcher = MarketDataFetcher()
        result = fetcher.fetch_bigdata_strategy_data()

        if result:
            print(f"大数投资策略数据获取成功")
            return {'status': result['status'], 'message': result['message']}
        
    except Exception as e:
        print(f"Error in bigdata_strategy_data_task: {str(e)}")
        try:
            self.retry()
        except MaxRetriesExceededError:
            return {'status': 'error', 'message': 'Max retries exceeded'}

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_convertible_bond_data(self):
    """可转债数据采集任务"""
    try:
        fetcher = ConvertibleBondMarketDataFetcher()
        # result = fetcher.fetch_convertible_bond_market_data()
        result = fetcher.fetch_convertible_bond_market_data_dongfangcaifu()

        if result:
            print(f"可转债数据获取成功: {result['message']}")
            return {'status': 'success'}
        
    except Exception as e:
        print(f"Error in convertible_bond_data_task: {str(e)}")
        try:
            self.retry()
        except MaxRetriesExceededError:
            return {'status': 'error', 'message': 'Max retries exceeded'}
        

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_bond_index_data(self):
    """可转债指数数据采集任务"""
    try:
        fetcher = ConvertibleBondMarketDataFetcher()
        result = fetcher.fetch_bond_index_data()

        if result:
            print(f"可转债指数数据获取成功")
            return {'status': 'success'}

    except Exception as e:
        print(f"Error in bond_index_data_task: {str(e)}")
        try:
            self.retry()
        except MaxRetriesExceededError:
            return {'status': 'error', 'message': 'Max retries exceeded'}