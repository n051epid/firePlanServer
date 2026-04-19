"""
Proxy Manager for akshare requests
解决 IP 封禁问题，支持多代理轮询、自动故障转移
"""
import os
import requests
import threading
import time
import logging
from typing import Optional, List, Dict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ProxyManager:
    """
    代理管理器
    - 支持 HTTP/HTTPS 代理
    - 支持认证代理 (user:pass@host:port)
    - 多代理轮询负载均衡
    - 自动故障转移
    - 代理使用统计
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        
        self._proxies: List[Dict] = []
        self._current_index = 0
        self._proxy_stats: Dict[str, Dict] = {}  # proxy -> {success: int, fail: int, consecutive_fails: int, last_used: float}
        self._failed_proxies: set = set()  # 连续失败次数过多的代理
        self._max_consecutive_fails = 3  # 连续失败次数阈值
        self._max_total_fails = 10  # 总失败次数阈值
        self._direct_mode = False  # 所有代理都失效时回退到直连
        self._lock = threading.Lock()
        
        self._load_proxies()
    
    def _load_proxies(self):
        """从环境变量加载代理列表"""
        proxy_list_env = os.environ.get('PROXY_LIST', '')
        
        if not proxy_list_env:
            logger.info("ProxyManager: 未配置 PROXY_LIST 环境变量，将使用直连模式")
            self._direct_mode = True
            return
        
        # 解析代理列表，格式: http://user:pass@host:port,http://host:port,...
        proxy_urls = [p.strip() for p in proxy_list_env.split(',') if p.strip()]
        
        if not proxy_urls:
            logger.info("ProxyManager: PROXY_LIST 环境变量为空，将使用直连模式")
            self._direct_mode = True
            return
        
        for url in proxy_urls:
            proxy_dict = self._parse_proxy_url(url)
            if proxy_dict:
                self._proxies.append(proxy_dict)
                self._proxy_stats[url] = {
                    'success': 0,
                    'fail': 0,
                    'consecutive_fails': 0,
                    'last_used': 0,
                    'url': url
                }
        
        if self._proxies:
            logger.info(f"ProxyManager: 成功加载 {len(self._proxies)} 个代理")
        else:
            logger.info("ProxyManager: 没有有效的代理配置，将使用直连模式")
            self._direct_mode = True
    
    def _parse_proxy_url(self, url: str) -> Optional[Dict]:
        """解析代理 URL，支持认证信息"""
        try:
            parsed = urlparse(url)
            
            # 确定代理协议
            scheme = parsed.scheme.lower()
            if scheme not in ('http', 'https'):
                logger.warning(f"ProxyManager: 不支持的代理协议 {scheme}")
                return None
            
            # 构建代理字典
            proxy_dict = {
                'http': url,
                'https': url.replace('http://', 'https://') if scheme == 'http' else url
            }
            
            return proxy_dict
        except Exception as e:
            logger.warning(f"ProxyManager: 解析代理 URL 失败 {url}: {e}")
            return None
    
    def get_proxy(self) -> Optional[Dict]:
        """
        获取下一个可用代理（轮询）
        返回 None 表示使用直连模式
        """
        with self._lock:
            if self._direct_mode or not self._proxies:
                return None
            
            # 过滤出可用的代理
            available_proxies = [p for p in self._proxies if p['http'] not in self._failed_proxies]
            
            if not available_proxies:
                logger.warning("ProxyManager: 所有代理都已标记为失效，回退到直连模式")
                self._direct_mode = True
                return None
            
            # 轮询选择代理
            attempts = 0
            while attempts < len(available_proxies):
                proxy = available_proxies[self._current_index % len(available_proxies)]
                self._current_index += 1
                
                if proxy['http'] not in self._failed_proxies:
                    proxy_url = proxy['http']
                    self._proxy_stats[proxy_url]['last_used'] = time.time()
                    return proxy
                
                attempts += 1
            
            # 如果所有代理都失效，回退到直连
            logger.warning("ProxyManager: 无法获取可用代理，回退到直连模式")
            self._direct_mode = True
            return None
    
    def mark_success(self, proxy_url: str):
        """标记代理使用成功"""
        with self._lock:
            if proxy_url in self._proxy_stats:
                stats = self._proxy_stats[proxy_url]
                stats['success'] += 1
                stats['consecutive_fails'] = 0
                
                # 如果之前所有代理都失败了，尝试恢复
                if proxy_url in self._failed_proxies:
                    self._failed_proxies.discard(proxy_url)
                    logger.info(f"ProxyManager: 代理恢复成功 {proxy_url} (成功: {stats['success']}, 失败: {stats['fail']})")
    
    def mark_fail(self, proxy_url: str):
        """标记代理使用失败"""
        with self._lock:
            if proxy_url in self._proxy_stats:
                stats = self._proxy_stats[proxy_url]
                stats['fail'] += 1
                stats['consecutive_fails'] += 1
                
                # 连续失败次数过多，标记为失效
                if stats['consecutive_fails'] >= self._max_consecutive_fails:
                    self._failed_proxies.add(proxy_url)
                    logger.warning(f"ProxyManager: 代理连续失败 {stats['consecutive_fails']} 次，标记为失效: {proxy_url}")
                    logger.warning(f"ProxyManager: 代理统计 - 成功: {stats['success']}, 失败: {stats['fail']}")
                elif stats['fail'] >= self._max_total_fails:
                    # 总失败次数过多
                    ratio = stats['fail'] / (stats['success'] + stats['fail'])
                    if ratio > 0.5:  # 失败率超过 50%
                        self._failed_proxies.add(proxy_url)
                        logger.warning(f"ProxyManager: 代理失败率过高 ({ratio:.1%})，标记为失效: {proxy_url}")
    
    def reset_direct_mode(self):
        """重置直连模式，尝试重新使用代理"""
        with self._lock:
            if self._failed_proxies and len(self._failed_proxies) < len(self._proxies):
                self._direct_mode = False
                self._failed_proxies.clear()
                logger.info("ProxyManager: 重置直连模式，尝试重新使用代理")
    
    def get_stats(self) -> Dict:
        """获取代理使用统计"""
        with self._lock:
            return {
                'total_proxies': len(self._proxies),
                'available_proxies': len([p for p in self._proxies if p['http'] not in self._failed_proxies]),
                'failed_proxies': len(self._failed_proxies),
                'direct_mode': self._direct_mode,
                'stats': dict(self._proxy_stats)
            }
    
    def is_using_proxy(self) -> bool:
        """检查是否正在使用代理模式"""
        return not self._direct_mode and bool(self._proxies)


class ProxyAdapter(requests.adapters.HTTPAdapter):
    """
    自定义 requests Adapter，自动使用代理管理器
    """
    
    def __init__(self, proxy_manager: ProxyManager, **kwargs):
        super().__init__(**kwargs)
        self.proxy_manager = proxy_manager
    
    def send(self, request, **kwargs):
        """重写 send 方法，注入代理"""
        proxy = self.proxy_manager.get_proxy()
        
        if proxy:
            kwargs['proxies'] = proxy
            proxy_url = proxy['http']
        else:
            # 直连模式，不设置代理
            proxy_url = None
        
        try:
            response = super().send(request, **kwargs)
            
            # 标记成功
            if proxy_url:
                self.proxy_manager.mark_success(proxy_url)
            
            return response
            
        except requests.exceptions.RequestException as e:
            # 标记失败
            if proxy_url:
                self.proxy_manager.mark_fail(proxy_url)
            
            # 尝试不使用代理重试一次（如果是代理问题）
            if proxy and not self.proxy_manager._direct_mode:
                logger.warning(f"ProxyManager: 代理请求失败，尝试直连: {e}")
                self.proxy_manager.mark_fail(proxy_url)
                
                # 不使用代理重试
                kwargs.pop('proxies', None)
                try:
                    response = super().send(request, **kwargs)
                    return response
                except:
                    pass
            
            raise


class AkshareProxyPatcher:
    """
    Monkey Patch akshare 的 HTTP 请求，使用代理
    """
    
    _patched = False
    _lock = threading.Lock()
    
    @classmethod
    def patch(cls):
        """执行 patch"""
        with cls._lock:
            if cls._patched:
                return
            
            cls._patched = True
        
        proxy_manager = ProxyManager()
        
        # 获取 akshare 使用的 session
        # akshare 内部创建自己的 session，我们通过修持久化连接适配器来实现
        
        # 创建一个自定义的 session 类
        class ProxiedSession(requests.Session):
            def __init__(self, pm):
                super().__init__()
                self._pm = pm
                # 替换适配器
                self.mount('http://', ProxyAdapter(pm))
                self.mount('https://', ProxyAdapter(pm))
        
        # patch requests.Session
        _original_Session = requests.Session
        
        def patched_Session(*args, **kwargs):
            session = _original_Session(*args, **kwargs)
            session.mount('http://', ProxyAdapter(proxy_manager))
            session.mount('https://', ProxyAdapter(proxy_manager))
            return session
        
        requests.Session = patched_Session
        
        # 同时 patch 一些常用的获取 session 的地方
        # akshare 可能在内部会调用 requests.get/post
        
        _original_get = requests.get
        _original_post = requests.post
        
        def patched_get(url, **kwargs):
            # 注入代理
            proxy = proxy_manager.get_proxy()
            if proxy:
                kwargs['proxies'] = proxy
                proxy_url = proxy['http']
            else:
                proxy_url = None
            
            try:
                response = _original_get(url, **kwargs)
                if proxy_url:
                    proxy_manager.mark_success(proxy_url)
                return response
            except requests.exceptions.RequestException as e:
                if proxy_url:
                    proxy_manager.mark_fail(proxy_url)
                raise
        
        def patched_post(url, **kwargs):
            # 注入代理
            proxy = proxy_manager.get_proxy()
            if proxy:
                kwargs['proxies'] = proxy
                proxy_url = proxy['http']
            else:
                proxy_url = None
            
            try:
                response = _original_post(url, **kwargs)
                if proxy_url:
                    proxy_manager.mark_success(proxy_url)
                return response
            except requests.exceptions.RequestException as e:
                if proxy_url:
                    proxy_manager.mark_fail(proxy_url)
                raise
        
        requests.get = patched_get
        requests.post = patched_post
        
        logger.info("ProxyManager: 成功 patch requests 模块")


class ProxyMixIn:
    """
    Mixin 类，为需要使用代理的方法提供支持
    """
    
    _proxy_manager = None
    
    @classmethod
    def get_proxy_manager(cls) -> ProxyManager:
        """获取代理管理器单例"""
        if cls._proxy_manager is None:
            cls._proxy_manager = ProxyManager()
            # 尝试 patch
            AkshareProxyPatcher.patch()
        return cls._proxy_manager
    
    @classmethod
    def get_proxied_session(cls) -> requests.Session:
        """获取配置了代理的 requests Session"""
        pm = cls.get_proxy_manager()
        
        session = requests.Session()
        session.mount('http://', ProxyAdapter(pm))
        session.mount('https://', ProxyAdapter(pm))
        
        return session


# 全局初始化函数
def init_proxy():
    """初始化代理系统"""
    pm = ProxyManager()
    if pm.is_using_proxy():
        AkshareProxyPatcher.patch()
        logger.info(f"ProxyManager: 初始化完成，使用 {len(pm._proxies)} 个代理")
    else:
        logger.info("ProxyManager: 初始化完成，使用直连模式")
    return pm
