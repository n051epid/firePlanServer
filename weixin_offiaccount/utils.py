import requests
import time
from django.core.cache import cache
from django.conf import settings
import json
import logging

logger = logging.getLogger(__name__)

class WeChatAccessToken:
    def __init__(self,appid,secret):
        self.appid = appid
        self.secret = secret
        self.cache_key = 'wechat_access_token_' + appid
        
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
wechat_token_qinglv = WeChatAccessToken(appid=settings.WECHAT_APP_ID_QINGLV, secret=settings.WECHAT_APP_SECRET_QINGLV)
wechat_token_black13eard = WeChatAccessToken(appid=settings.WECHAT_APP_ID_BLACK13EARD, secret=settings.WECHAT_APP_SECRET_BLACK13EARD)

def sync_to_wechat_draft(wx_account, articles):
    """
    同步文章到微信公众号草稿箱
    
    Args:
        wx_account (str): 微信公众号账号标识
        articles (dict): 包含文章数据的字典
            {
                'titles': [title_0, title_1, ...],
                'contents': [content_0, content_1, ...],
                'digests': [digest_0, digest_1, ...],
                'thumb_media_ids': [thumb_media_id_0, thumb_media_id_1, ...]
            }
    """
    try:
        # 获取access_token
        if (wx_account == 'fire_qinglv'):
            access_token = wechat_token_qinglv.get_access_token()
            author = settings.WECHAT_AUTHOR_NAME_QINGLV
        else:
            access_token = wechat_token_black13eard.get_access_token()
            author = settings.WECHAT_AUTHOR_NAME_BLACK13EARD
        
        # 构建文章数据
        articles_data = []
        for i in range(len(articles['titles'])):
            articles_data.append({
                "title": articles['titles'][i],
                "content": articles['contents'][i],
                "digest": articles['digests'][i],
                "author": author,
                "content_source_url": "",
                "thumb_media_id": articles['thumb_media_ids'][i],
                "need_open_comment": 1,
                "only_fans_can_comment": 0
            })
        
        articles_data = {
            "articles": articles_data
        }
        
        # 构建请求URL
        url = f"https://api.weixin.qq.com/cgi-bin/draft/add?access_token={access_token}"
        
        # 发送请求时指定编码和内容类型
        response = requests.post(
            url,
            data=json.dumps(articles_data, ensure_ascii=False).encode('utf-8'),
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )

        result = response.json()
        
        if 'media_id' in result:
            logger.info(f"Successfully synced to WeChat({wx_account}) draft: {result['media_id']}")
            return True, result['media_id']
        else:
            logger.error(f"Failed to sync to WeChat({wx_account}) draft: {result}")
            return False, result.get('errmsg', 'Unknown error')
            
    except Exception as e:
        logger.error(f"Error syncing to WeChat({wx_account}) draft: {str(e)}")
        return False, str(e)


