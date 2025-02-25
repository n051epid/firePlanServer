from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

class WeixinOffiaccountConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'weixin_offiaccount'

    def ready(self):
        logger.info("WeixinOffiaccount app is ready")
