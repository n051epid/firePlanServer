from django.core.management.base import BaseCommand
from fire_100UpPlan.tasks import fetch_daily_market_data
from asgiref.sync import sync_to_async
import asyncio

class Command(BaseCommand):
    help = '手动触发市场数据采集'

    def handle(self, *args, **options):
        try:
            # 使用 sync_to_async 包装同步函数
            async_task = sync_to_async(fetch_daily_market_data.delay)
            
            # 创建事件循环并运行任务
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(async_task())
            
            self.stdout.write(self.style.SUCCESS('数据采集成功'))
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'发生错误: {str(e)}')
            ) 