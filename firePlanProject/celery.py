import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'firePlanProject.settings')

app = Celery('firePlanProject')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()