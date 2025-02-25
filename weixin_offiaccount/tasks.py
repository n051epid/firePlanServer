from celery import shared_task
from django.utils import timezone
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

@shared_task(
    name='weixin_offiaccount.tasks.generate_daily_article',
    bind=True,
    ignore_result=False,
    max_retries=3,
    default_retry_delay=300  # 5分钟后重试
)
def generate_daily_article(self):
    """
    生成每周文章并同步到微信公众号草稿箱
    Schedule: 每周日 {settings.CELERY_BEAT_HOURS}:45 执行
    """
    try:
        # 延迟导入，确保 Django apps 已完全加载
        from .views import MarketValuationSyncView
        from rest_framework.request import Request
        from django.http import HttpRequest
        
        logger.info(f"[{timezone.localtime()}] Starting daily article generation...")
        
        # 创建视图实例
        view = MarketValuationSyncView()
        
        # 创建模拟请求对象
        mock_request = HttpRequest()
        mock_request.META = {
            'HTTP_TIMEOUT': '300',  # 5分钟超时
            'SERVER_NAME': settings.ALLOWED_HOST,
            'SERVER_PORT': '443',
            'HTTP_X_FORWARDED_PROTO': 'https',
        }
        request = Request(mock_request)
        
        # 调用视图方法
        response = view.get(request)
        
        if response.status_code == 200:
            logger.info(f"[{timezone.localtime()}] Successfully generated and synced daily article")
            return {
                'status': 'success',
                'message': 'Article generated and synced successfully',
                'data': response.data,
                'timestamp': timezone.localtime().isoformat()
            }
        else:
            error_msg = f"Failed to generate daily article: {response.data}"
            logger.error(f"[{timezone.localtime()}] {error_msg}")
            # 如果失败，尝试重试
            raise self.retry(
                exc=Exception(error_msg),
                countdown=300,  # 5分钟后重试
                max_retries=3
            )
            
    except Exception as e:
        error_msg = f"Error in generate_daily_article task: {str(e)}"
        logger.error(f"[{timezone.localtime()}] {error_msg}")
        # 如果是重试异常，直接抛出
        if isinstance(e, self.retry.RetryError):
            raise e
        # 其他异常尝试重试
        raise self.retry(exc=e, countdown=300, max_retries=3) 