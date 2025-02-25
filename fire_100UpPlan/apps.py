from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class Fire100UpplanConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'fire_100UpPlan'
    
    def ready(self):
        """
        应用程序初始化时的配置
        """
        # 初始化日志配置
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

