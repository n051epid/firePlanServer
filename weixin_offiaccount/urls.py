from django.urls import path
from .views import WeChatAPIView, WechatAccountView, MarketValuationSyncView,MarketValuationSyncView2,KimiChatView

urlpatterns = [
    # path('', WeChatAPIView.as_view(), name='wechat_login'), # 微信回调 URL 设置验证使用，可废弃
    path('', WechatAccountView.as_view(), name='wechat_account'),
    # path('kimi-chat/', KimiChatView.as_view(), name='kimi_chat'),
    path('sync-market-valuation/', MarketValuationSyncView.as_view(), name='sync_market_valuation'),
] 