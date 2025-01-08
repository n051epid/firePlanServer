from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from django.contrib.auth.models import User, Group
from rest_framework_simplejwt.tokens import RefreshToken
from django.core.cache import cache
from django.conf import settings
from django.http import HttpResponse
import hashlib
import xmltodict
import time
import logging
import random
import string
from fire_100UpPlan.models import Membership, MembershipType
from fire_100UpPlan.views_fireplan.market_observation import MarketValuationView
from .utils import wechat_token, sync_to_wechat_draft
from datetime import datetime
import html

logger = logging.getLogger(__name__)

class WeChatAPIView(APIView):
    # 仅在修改微信回调 URL 时，验证使用
    def get(self, request):
        # 获取参数
        signature = request.GET.get('signature', '')
        timestamp = request.GET.get('timestamp', '')
        nonce = request.GET.get('nonce', '')
        echostr = request.GET.get('echostr', '')
        
        # 打印微信access_token，方便手动验证
        access_token = wechat_token.get_access_token()
        print("access_token: ", access_token)

        # 微信公众号的 token
        token = settings.WECHAT_TOKEN
        
        # 1. 将 token、timestamp、nonce 三个参数进行字典序排序
        temp_list = [token, timestamp, nonce]
        temp_list.sort()
        
        # 2. 将三个参数字符串拼接成一个字符串
        temp_str = ''.join(temp_list)
        
        # 3. 进行 sha1 加密
        sha1 = hashlib.sha1()
        sha1.update(temp_str.encode('utf-8'))
        hashcode = sha1.hexdigest()
        
        # 4. 对比 signature
        if hashcode == signature:
            # 5. 验证通过，返回 echostr
            return HttpResponse(echostr)
        else:
            return HttpResponse("验证失败")


class WechatLoginView(APIView):
    def get(self, request):
        verification_code = request.GET.get('verification_code')
        
        if not verification_code:
            verification_code = ''.join(random.choices(string.ascii_letters + string.digits, k=3))
            # 使用 cache 替代 session
            cache_key = f'wechat_login_{verification_code}'
            cache.set(
                cache_key,
                {
                    'timestamp': time.time(),
                    'verification_code': verification_code,
                    'openid': None
                },
                timeout=300  # 5分钟过期
            )

            return Response({
                'code': 0,
                'data': {
                    'qr_code_url': settings.WECHAT_QRCODE_URL,
                    'verification_code': verification_code
                },
                'error': None,
                'message': 'Success'
            })
        
        # 从 cache 中查找
        cache_key = f'wechat_login_{verification_code}'
        session_data = cache.get(cache_key)
        logger.info('session_data: ',session_data)

        if not session_data:
            #不做任何处理
            return Response({
                'code': 0,
                'data': {'status': 'waiting'},
                'error': None,
                'message': 'Waiting for scan'
            })
        
        # 检查session是否过期（例如 5分钟）
        if time.time() - session_data['timestamp'] > 300:
            cache_key = f'wechat_login_{verification_code}'
            cache.delete(cache_key)
            return Response({
                'code': 1,
                'data': None,
                'error': 'Session expired',
                'message': 'Failed'
            }, status=status.HTTP_400_BAD_REQUEST)
            
        openid = session_data.get('openid')
        if not openid:
            return Response({
                'code': 0,
                'data': {'status': 'waiting'},
                'error': None,
                'message': 'Waiting for scan'
            })
            
        try:
            # 查找用户
            user = User.objects.get(username=f'wechat_{openid}')
            
            # 生成JWT令牌
            refresh = RefreshToken.for_user(user)
            
            # 删除缓存
            cache_key = f'wechat_login_{verification_code}'
            cache.delete(cache_key)
            
            return Response({
                'code': 0,
                'data': {
                    'status': 'success',
                    'accessToken': str(refresh.access_token),
                    'refreshToken': str(refresh)
                },
                'error': None,
                'message': 'Login Successful'
            })
            
        except User.DoesNotExist:
            return Response({
                'code': 1,
                'data': None,
                'error': 'User not found',
                'message': 'User not found'
            }, status=status.HTTP_404_NOT_FOUND)

    def post(self, request):
        try:
            # 对请求来源进行验证，检查 signature
            signature = request.GET.get('signature', '')
            timestamp = request.GET.get('timestamp', '')
            nonce = request.GET.get('nonce', '')
            token = settings.WECHAT_TOKEN
            temp_list = [token, timestamp, nonce]
            temp_list.sort()
            temp_str = ''.join(temp_list)
            sha1 = hashlib.sha1()
            sha1.update(temp_str.encode('utf-8'))
            hashcode = sha1.hexdigest()

            if hashcode != signature:
                return HttpResponse("验证失败") # 验证失败，返回403
            
            # 从请求体获取XML数据
            xml_data = request.body
            xml_dict = xmltodict.parse(xml_data)['xml']
            print("xml_dict: ", xml_dict)

            # 获取OpenID和消息内容
            openid = xml_dict.get('FromUserName')
            content = xml_dict.get('Content', '').strip()
            
            # 获取消息类型，如果为 text 则进行消息处理
            msg_type = xml_dict.get('MsgType')
            if msg_type == 'text':
                # 从 cache 中查找
                cache_key = f'wechat_login_{content}'
                session_data = cache.get(cache_key)

                if session_data:
                    session_data['openid'] = openid
                    cache.set(cache_key, session_data, timeout=300)

                    # 查找或创建用户
                    user, created = User.objects.get_or_create(
                        username=f'wechat_{openid}',
                        defaults={'is_active': True}
                    )
                    
                    # 如果是新用户，创建免费会员资格
                    if created:
                        # 添加到默认组
                        default_group = Group.objects.get(name=settings.DEFAULT_USER_GROUPS['NORMAL_USER'])
                        user.groups.add(default_group)
                        # 添加会员
                        user_membership_type = MembershipType.objects.get(name="Premium Monthly")
                        Membership.objects.create(
                            user=user,
                            membership_type=user_membership_type,
                            is_active=True
                        )

                    # 构建回复消息
                    reply_msg = f"""<xml>
                        <ToUserName><![CDATA[{openid}]]></ToUserName>
                        <FromUserName><![CDATA[{xml_dict['ToUserName']}]]></FromUserName>
                        <CreateTime>{int(time.time())}</CreateTime>
                        <MsgType><![CDATA[text]]></MsgType>
                        <Content><![CDATA[欢迎回来！]]></Content>
                    </xml>"""
                
                    return HttpResponse(reply_msg, content_type='application/xml')
                
            # 如果是 event 则进行事件处理
            elif msg_type == 'event':
                # 获取事件类型
                event_type = xml_dict.get('Event')
                event_key = xml_dict.get('EventKey')
                # 如果 Event 是 VIEW 且 EventKey 中包含 qinglv.online 则进行处理
                if event_type == 'VIEW' and 'qinglv.online' in event_key:
                    # 访问事件
                    print("访问事件: 应该进行登陆")

                    # cache_key = 'wechat_login_auto_login'
                    # # 将 openid 存入 cache ，方便进行自动登录
                    # cache.set(
                    #     cache_key,
                    #     {
                    #         'timestamp': time.time(),
                    #         'openid': openid
                    #     },
                    #     timeout=300  # 5分钟过期
                    # )

                    # 查找或创建用户
                    user, created = User.objects.get_or_create(
                        username=f'wechat_{openid}',
                        defaults={'is_active': True}
                    )
                    
                    # 如果是新用户，创建免费会员资格
                    if created:
                        # 添加到默认组
                        default_group = Group.objects.get(name=settings.DEFAULT_USER_GROUPS['NORMAL_USER'])
                        user.groups.add(default_group)
                        # 添加会员
                        user_membership_type = MembershipType.objects.get(name="Premium Monthly")
                        Membership.objects.create(
                            user=user,
                            membership_type=user_membership_type,
                            is_active=True
                        )
                    
                    return HttpResponse("success")

                elif event_type == 'subscribe':
                    # 构建回复消息
                    reply_msg = f"""<xml>
                        <ToUserName><![CDATA[{openid}]]></ToUserName>
                        <FromUserName><![CDATA[{xml_dict['ToUserName']}]]></FromUserName>
                        <CreateTime>{int(time.time())}</CreateTime>
                        <MsgType><![CDATA[text]]></MsgType>
                        <Content><![CDATA[轻旅是一种生活状态，FIRE是一种生活方式，而 PlanB 则是一种生活策略。FIRE轻旅，欢迎关注！]]></Content>
                    </xml>"""
                    return HttpResponse(reply_msg, content_type='application/xml')
            
        except Exception as e:
            logger.error(f"微信消息处理错误: {str(e)}")
            # 如果处理出错，返回success避免微信服务器重试
            return HttpResponse("success")


class MarketValuationSyncView(APIView):
    def get(self, request):
        try:
            # 从 fire_100UpPlan 获取市场估值数据
            market_view = MarketValuationView()
            response = market_view.get(request)
            
            if response.status_code != 200:
                return response
                
            data = response.data['data']
            
            # 从 index_data 中获取所有的指数数据
            index_data = data['index_data']
            market_overview = data['market_overview']
            market_sentiment = data['market_sentiment']

            content = f"""
            <section style="font-size: 12px; padding: 10px;">
                <section style="margin: 20px 0;">
                    <h3 style="color: #333; font-weight: bold;">市场整体情况</h3>
                    <p>最新温度：<span style="color: {get_status_color(market_sentiment['market_temperature'].get('market_sentiment', '-'))}; font-weight: bold;">{int(market_sentiment['market_temperature']['temperature'])}°</span>，
                       温度{get_trend_text(market_sentiment['market_temperature'].get('temperature_trend', '-'))}，全市场处于 <span style="color: {get_status_color(market_sentiment['market_temperature'].get('market_sentiment', '-'))}; font-weight: bold;">{get_status_text(market_sentiment['market_temperature'].get('market_sentiment', '-'))}</span> 状态。</p>
                    <table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;">
                        <tr style="background-color: #f1f8ff;">
                            <th style="padding: 10px; text-align: left; border: 1px solid #eee;">指标</th>
                            <th style="padding: 10px; text-align: center; border: 1px solid #eee;">数值</th>
                            <th style="padding: 10px; text-align: center; border: 1px solid #eee;">历史区间位置</th>
                        </tr>
                        <tr>
                            <td style="padding: 10px; border: 1px solid #eee;">沪深市净率(PB)</td>
                            <td style="padding: 10px; text-align: center; border: 1px solid #eee;">{market_overview['pb']['ratio']:.2f} 倍</td>
                            <td style="padding: 10px; text-align: center; border: 1px solid #eee;">{market_overview['pb']['range_percentile']:.2f}%</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; border: 1px solid #eee;">沪深市盈率(PE)</td>
                            <td style="padding: 10px; text-align: center; border: 1px solid #eee;">{market_overview['pe']['ratio']:.2f} 倍</td>
                            <td style="padding: 10px; text-align: center; border: 1px solid #eee;">{market_overview['pe']['range_percentile']:.2f}%</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; border: 1px solid #eee;">全市场股息率</td>
                            <td style="padding: 10px; text-align: center; border: 1px solid #eee;">{market_overview.get('dividend_ratio', '-'):.2f}%</td>
                            <td style="padding: 10px; text-align: center; border: 1px solid #eee;">-</td>
                        </tr>
                    </table>
                </section>

                <section style="margin: 20px 0;">
                    <h3 style="color: #333; font-weight: bold;">主要指数估值</h3>
                    <table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;">
                        <tr style="background-color: #f1f8ff;">
                            <th style="padding: 10px; text-align: left; border: 1px solid #eee;">指数名称</th>
                            <th style="padding: 10px; text-align: center; border: 1px solid #eee;">PE-TTM</th>
                            <th style="padding: 10px; text-align: center; border: 1px solid #eee;">指数温度</th>
                        </tr>
                        {generate_index_rows(index_data)}
                    </table>
                </section>
            </section>
            """

            digest = f"""最新温度：{int(market_sentiment['market_temperature']['temperature'])}°，
            温度{get_trend_text(market_sentiment['market_temperature'].get('temperature_trend', '-'))}，
            全市场处于{get_status_text(market_sentiment['market_temperature'].get('market_sentiment', '-'))}状态
            """

            # 同步到微信公众号草稿箱
            success, result = sync_to_wechat_draft(
                title=f"今日市场估值（{market_overview['date']}）",
                content=content,
                digest=digest
            )
            
            if not success:
                return Response({
                    'code': 1,
                    'error': result,
                    'message': 'Failed to sync to WeChat draft'
                }, status=500)
            
            return Response({
                'code': 0,
                'data': {
                    'media_id': result
                },
                'message': 'Successfully synced to WeChat draft'
            })
            
        except Exception as e:
            logger.error(f"Error in MarketValuationSyncView: {str(e)}")
            return Response({
                'code': 1,
                'error': str(e),
                'message': 'Failed'
            }, status=500)

def get_status_color(status):
    """根据状态返回对应的颜色"""
    color_map = {
        'High': '#ff4d4f',    # 红色 - 高估
        'Medium': '#ff9900',   # 橙色 - 中估
        'Low': '#52c41a'      # 绿色 - 低估
    }
    return color_map.get(status, '#ff9900')  # 默认返回橙色

def get_status_text(status):
    """根据状态返回对应的中文描述"""
    text_map = {
        'High': '高估',
        'Medium': '中估',
        'Low': '低估'
    }
    return text_map.get(status, '-')

def get_trend_text(trend):
    """根据趋势返回对应的中文描述"""
    text_map = {
        '↑': '上升',
        '↓': '下降',
        '→': '不变'
    }
    return text_map.get(trend, '-')

def generate_index_rows(index_data):
    """生成指数行数据的辅助函数"""
    rows = []
    for data in index_data:  # 直接遍历列表
        if 'name' in data and 'code' in data:  # 确保数据中包含必要字段
            row = f"""
            <tr>
                <td style="padding: 10px; border: 1px solid #eee;">{data['name']}({data['code']})</td>
                <td style="padding: 10px; text-align: center; border: 1px solid #eee;">{data['pe_ttm_ratio']:.2f}</td>
                <td style="padding: 10px; text-align: center; border: 1px solid #eee;">{data['percentile']:.2f} {data.get('trend', '-')}</td>
            </tr>
            """
            rows.append(row)
    
    return '\n'.join(rows)