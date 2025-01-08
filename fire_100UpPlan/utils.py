import requests
from django.conf import settings
import json
import logging

logger = logging.getLogger(__name__)

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
