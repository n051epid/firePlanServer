from __future__ import absolute_import, unicode_literals
import os
import logging
from celery import Celery
from django.conf import settings

# 设置日志记录器
logger = logging.getLogger(__name__)

# 设置默认的 Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'firePlanProject.settings')

app = Celery('firePlanProject')

# 使用 Django 的设置配置 Celery
app.config_from_object('django.conf:settings', namespace='CELERY')

# 自动从所有已注册的 Django app 配置中加载任务
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

@app.task(bind=True, ignore_result=True)
def debug_task(self):
    # 修改日志记录方式
    logger.info('Request: %s', self.request)
