import requests
import time
from django.core.cache import cache
from django.conf import settings
import json
import logging

logger = logging.getLogger(__name__)

class WeChatAccessToken:
    def __init__(self):
        self.appid = settings.WECHAT_APP_ID
        self.secret = settings.WECHAT_APP_SECRET
        self.cache_key = 'wechat_access_token'
        
    def _fetch_access_token(self):
        """从微信服务器获取新的 access_token"""
        url = f"https://api.weixin.qq.com/cgi-bin/token"
        params = {
            'grant_type': 'client_credential',
            'appid': self.appid,
            'secret': self.secret
        }
        
        try:
            response = requests.get(url, params=params)
            result = response.json()
            
            if 'access_token' in result:
                # 成功获取 access_token，设置缓存
                access_token = result['access_token']
                expires_in = result.get('expires_in', 7200)  # 默认7200秒
                # 提前5分钟过期，避免临界点问题
                cache.set(self.cache_key, access_token, expires_in - 300)
                return access_token
            else:
                logger.error(f"Failed to fetch access token: {result}")
                if result.get('errcode') == 40164:
                    logger.error("IP not in whitelist. Please add server IP to WeChat MP IP whitelist.")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching access token: {str(e)}")
            return None
            
    def get_access_token(self):
        """获取 access_token，如果缓存中没有或已过期则重新获取"""
        access_token = cache.get(self.cache_key)
        
        if not access_token:
            access_token = self._fetch_access_token()
            
        return access_token

# 创建单例实例
wechat_token = WeChatAccessToken()

def verify_paypal_webhook(transmission_id, timestamp, webhook_signature, event_body):
    logger.info(f"Verifying webhook: {transmission_id}, {timestamp}")
    
    # 使用配置中的 API 基础 URL
    token_url = f"{settings.PAYPAL_API_BASE}/v1/oauth2/token"
    verify_url = f"{settings.PAYPAL_API_BASE}/v1/notifications/verify-webhook-signature"

    # 获取 access token
    auth_response = requests.post(
        token_url,
        auth=(settings.PAYPAL_CLIENT_ID, settings.PAYPAL_CLIENT_SECRET),
        data={'grant_type': 'client_credentials'}
    )
    logger.info(f"Auth response: {auth_response.text}")
    access_token = auth_response.json()['access_token']

    # 如果 event_body 是 bytes 类型，将其解码为字符串
    if isinstance(event_body, bytes):
        event_body = event_body.decode('utf-8')

    # 确保 event_body 是一个字典
    if isinstance(event_body, str):
        event_body = json.loads(event_body)

    # 验证 webhook
    verify_response = requests.post(
        verify_url,
        headers={'Authorization': f'Bearer {access_token}'},
        json={
            'transmission_id': transmission_id,
            'transmission_time': timestamp,
            'cert_url': f"{settings.PAYPAL_API_BASE}/v1/notifications/certs/CERT-360caa42-fca2a594-a5cafa77",
            'auth_algo': 'SHA256withRSA',
            'transmission_sig': webhook_signature,
            'webhook_id': settings.PAYPAL_WEBHOOK_ID,
            'webhook_event': event_body
        }
    )

    logger.info(f"Verification response: {verify_response.text}")

    return verify_response.json()['verification_status'] == 'SUCCESS'
