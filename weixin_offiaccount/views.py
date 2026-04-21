from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from django.contrib.auth.models import User, Group
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework.permissions import IsAuthenticated
from django.core.cache import cache
from django.conf import settings
from openai import OpenAI
from django.http import HttpRequest
from django.http import HttpResponse
from rest_framework.request import Request
import json
from django.http import JsonResponse
import hashlib
import xmltodict
import time
import logging
import random
import string
from fire_100UpPlan.models import Membership, MembershipType
from fire_100UpPlan.views_fireplan.market_observation import MarketValuationView,ConvertibleBondMarketDataView
from .utils import wechat_token_qinglv, wechat_token_black13eard, sync_to_wechat_draft, error_response
from datetime import datetime
import html
from django.http import StreamingHttpResponse
import asyncio



logger = logging.getLogger(__name__)

class KimiChatView(APIView):
    """Kimi 聊天视图，整段式问答。用于公众号内容总结输出"""
    name = "AI Assistant Chat" # 这将被用作链接文本
    permission_classes = [IsAuthenticated]

    def __init__(self):
        super().__init__()
        self.client = OpenAI(
            api_key=settings.KIMI_API_KEY,
            base_url="https://api.moonshot.cn/v1"
        )

    def _chat_with_kimi(self, messages):
        """私有方法，处理与 Kimi API 的通信
        
        Args:
            messages: 消息列表
            
        Yields:
            str: 生成的 SSE 格式响应
        """
        # 初始化响应内容
        content = ""
        max_retries = 3
        retry_count = 0
        
        while True:
            try:
                completion = self.client.chat.completions.create(
                    model="moonshot-v1-auto",
                    messages=[
                        *messages
                    ],
                    temperature=0.0,
                    tools=[
                        {
                            "type": "builtin_function",
                            "function": {
                                "name": "$web_search",
                            },
                        }
                    ],
                    timeout=100  # 设置100秒超时
                )
                
                choice = completion.choices[0]
                
                # 如果需要调用工具
                if choice.finish_reason == "tool_calls":
                    for tool_call in choice.message.tool_calls:
                        if tool_call.function.name == "$web_search":
                            # 将工具调用结果添加到消息列表中
                            messages.append(choice.message)
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_call.id,
                                "name": tool_call.function.name,
                                "content": json.dumps(json.loads(tool_call.function.arguments))
                            })
                    continue
                
                # 如果生成完成，返回内容
                content += choice.message.content
                break
                
            except Exception as e:
                logger.error(f"Error in _chat_with_kimi: {str(e)}")
                retry_count += 1
                if retry_count >= max_retries:
                    raise Exception(f"Failed after {max_retries} retries: {str(e)}")
                time.sleep(1)  # 重试前等待1秒
                continue
        
        yield f"data: {content}\n\n"
        yield "data: [DONE]\n\n"

    def post(self, request, *args, **kwargs):
        try:
            messages = request.data.get("messages", [])
            
            if not messages:
                return error_response(1, "Messages are required")

            # 获取生成的内容
            content = ""
            for chunk in self._chat_with_kimi(messages):
                if chunk.startswith("data: ") and not chunk.endswith("[DONE]\n\n"):
                    content += chunk.replace("data: ", "").strip()

            return Response({"content": content})
            
        except Exception as e:
            logger.error(f"Error in KimiChatView: {str(e)}")
            return error_response(1, "Internal server error", 500)
        

class WeChatAPIView(APIView):
    # 仅在修改微信回调 URL 时，验证使用
    def get(self, request):
        # 获取参数
        signature = request.GET.get('signature', '')
        timestamp = request.GET.get('timestamp', '')
        nonce = request.GET.get('nonce', '')
        echostr = request.GET.get('echostr', '')
        
        # 打印微信access_token，方便手动验证
        # access_token = wechat_token_qinglv.get_access_token()
        access_token = wechat_token_black13eard.get_access_token()
        print("access_token: ", access_token)

        # 微信公众号的 token
        token = settings.WECHAT_TOKEN_QINGLV
        # token = settings.WECHAT_TOKEN_BLACK13EARD
        
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


class WechatAccountView(APIView):
    def get(self, request):
        logger.info("Received request at /v1/wechat/")
        logger.debug("Request parameters: %s", request.GET)
        
        # 首先检查是否是微信服务器的验证请求
        signature = request.GET.get('signature')
        timestamp = request.GET.get('timestamp')
        nonce = request.GET.get('nonce')
        echostr = request.GET.get('echostr')
        
        # 如果是微信验证请求
        if all([signature, timestamp, nonce, echostr]):
            logger.info("Received WeChat verification request: signature=%s, timestamp=%s, nonce=%s, echostr=%s",
                       signature, timestamp, nonce, echostr)
            
            # token = settings.WECHAT_TOKEN_QINGLV
            token = settings.WECHAT_TOKEN_BLACK13EARD
            
            temp_list = [token, timestamp, nonce]
            temp_list.sort()
            temp_str = ''.join(temp_list)
            
            sha1 = hashlib.sha1()
            sha1.update(temp_str.encode('utf-8'))
            hashcode = sha1.hexdigest()
            
            if hashcode == signature:
                return HttpResponse(echostr)
            return HttpResponse("验证失败")
            
        # 如果是普通的 GET 请求，继续原有的验证码逻辑
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
                    'qr_code_url': settings.WECHAT_QRCODE_URL_QINGLV,
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
            token = settings.WECHAT_TOKEN_QINGLV
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
                
                elif event_type == 'CLICK':
                    # 获取点击事件的 key
                    click_key = xml_dict.get('EventKey')
                    print("click_key: ", click_key)

                    # 根据 key 进行处理
                    if click_key == 'coming_soon':
                        # 构建回复消息
                        reply_msg = f"""<xml>
                            <ToUserName><![CDATA[{openid}]]></ToUserName>
                            <FromUserName><![CDATA[{xml_dict['ToUserName']}]]></FromUserName>
                            <CreateTime>{int(time.time())}</CreateTime>
                            <MsgType><![CDATA[text]]></MsgType>
                            <Content><![CDATA[开发中，敬请期待！如果您有好的建议，欢迎给我们留言。]]></Content>
                        </xml>"""
                        return HttpResponse(reply_msg, content_type='application/xml')
            
        except Exception as e:
            logger.error(f"微信消息处理错误: {str(e)}")
            # 如果处理出错，返回success避免微信服务器重试
            return HttpResponse("success")


class MarketValuationSyncView(APIView):
    # 获取市场估值数据及可转债数据，生成文本内容，推送到微信公众号
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            # 设置更长的超时时间
            request.META['HTTP_TIMEOUT'] = '300'  # 5分钟超时

            # 构建第一篇文章：市场整体估值。从 fire_100UpPlan 获取市场估值数据
            market_view = MarketValuationView()
            response = market_view.get(request)
            
            if response.status_code != 200:
                return response
                
            data = response.data['data']
            
            # 从 index_data 中获取所有的指数数据
            index_data = data['index_data'] # 指数数据
            market_overview = data['market_overview'] # 市场整体估值
            market_sentiment = data['market_sentiment'] # 市场情绪
            industry_valuation = data['industry_valuation'] # 行业整体估值

            # 构建标题
            # 将日期格式从 YYYY-MM-DD 改为 MMDD
            date_obj = datetime.strptime(market_overview['date'], '%Y-%m-%d')
            formatted_date = date_obj.strftime('%m%d')
            title_0=f"今日市场估值（{formatted_date}）"

            # 构建摘要
            market_temp = market_sentiment.get('market_temperature', {})
            temperature_value = market_temp.get('temperature', 0)
            temperature_trend = market_temp.get('temperature_trend', '-')
            market_sentiment_status = market_temp.get('market_sentiment', '-')
            
            digest_0 = f"""最新温度：{int(temperature_value) if temperature_value else 0}° {get_trend_text(temperature_trend)}，全市场处于{get_status_text(market_sentiment_status)}状态 """
            thumb_media_id_0 = settings.WECHAT_DEFAULT_THUMB_MEDIA_ID_BLACK13EARD

            # 构建文章结尾签名
            bottom_content = f"""<p style="text-align: right;"><span style="font-size: 12px; color: #999; font-style: italic;">**以上来源于公开数据，仅供参考**</span></p>"""
            bottom_signature_black13eard = f"""<section style="font-size: 14px; line-height: 2; color: rgb(62, 62, 62); visibility: visible;"><p style="visibility: visible;"><br style="visibility: visible;"></p><section style="margin-top: 0.5em; margin-bottom: 0.5em; visibility: visible;"><section style="display: inline-block; vertical-align: top; margin-top: 0.5em; margin-bottom: 0.5em; width: 100%; visibility: visible;"><section style="visibility: visible;"><section style="box-shadow: rgba(159, 160, 160, 0.5) 0px 0px 10px; padding: 10px; display: inline-block; vertical-align: top; visibility: visible;"><section style="box-shadow: rgba(0, 0, 0, 0.29) 0px 0px 10px inset; padding: 7px; visibility: visible;"><section style="text-align: center; line-height: 0; visibility: visible;"><section style="vertical-align: middle; display: inline-block; line-height: 0; visibility: visible;"><img class="rich_pages wxw-img" data-imgfileid="100000125" data-ratio="0.75" data-s="300,640" data-src="https://mmbiz.qpic.cn/mmbiz_jpg/Y1ciargAZx1CoYhEz7FKDjJtPCXgLXOqI2wEGlfrjYeA7qKic9e3DmfYo9ic6yUK7GicPiaKyFLrN13DvuIDCJianG9A/640?wx_fmt=jpeg&amp;from=appmsg" data-type="jpeg" data-w="1080" style="vertical-align: middle; width: 643px !important; height: auto !important; visibility: visible !important;" data-original-style="vertical-align: middle;width: 100%;" data-index="1" src="https://mmbiz.qpic.cn/mmbiz_jpg/Y1ciargAZx1CoYhEz7FKDjJtPCXgLXOqI2wEGlfrjYeA7qKic9e3DmfYo9ic6yUK7GicPiaKyFLrN13DvuIDCJianG9A/640?wx_fmt=jpeg&amp;from=appmsg&amp;tp=webp&amp;wxfrom=5&amp;wx_lazy=1&amp;wx_co=1" _width="100%" crossorigin="anonymous" alt="图片" data-fail="0"></section></section><section style="clear: both; visibility: visible;"><svg viewBox="0 0 1 1" style="float: left; line-height: 0; width: 0px; vertical-align: top; visibility: visible;"></svg></section></section></section></section><section style="padding-top: 5px; padding-bottom: 5px; visibility: visible;"><section style="font-size: 8px; text-align: right; visibility: visible;"><p style="visibility: visible;"><br style="visibility: visible;"></p></section></section><section style="color: rgba(0, 0, 0, 0.61);text-align: right;"><p>蜀之鄙有二僧，其一贫，其一富。</p><p>贫者语于富者曰：「吾欲之南海，何如？」</p><p>富者曰：「子何恃而往？」</p><p>曰：「吾一瓶一钵足矣。」</p><p>富者曰：「吾数年来欲买舟而下，犹未能也。</p><p>子何持而往！」</p><p>越明年，贫者自南海还。</p><p><br></p></section></section></section><section class="mp_profile_iframe_wrp"><mp-common-profile class="js_uneditable custom_select_card mp_profile_iframe mp_common_widget js_wx_tap_highlight" data-pluginname="mpprofile" data-id="Mzk1NzM0OTAzMw==" data-headimg="http://mmbiz.qpic.cn/mmbiz_png/Y1ciargAZx1CcpZhWPeQd098IAbG69FXXqHczLt8mMW5KSnfBmVrq4qrIsxWPLvlpLnrn0wO0KVmicWtTDj4YJrQ/300?wx_fmt=png&amp;wxfrom=19" data-nickname="FIRE轻旅" data-alias="fireqinglv" data-signature="践行 F.I.R.E 理念，倡导简约自由的轻旅生活。" data-from="0" data-is_biz_ban="0" data-service_type="1" data-origin_num="0" data-isban="0" data-biz_account_status="0" data-index="0"></mp-common-profile></section></section>"""

            # 构建正文内容
            content_0 = f"""
<section style="font-size: 14px; padding: 10px;"><section style="margin: 10px 0;"><h3 style="color: #333; font-weight: bold;">◆ 市场整体估值</h3><br>
<span style="font-size: 14px;">最新温度：<span style="color: {get_status_color(market_sentiment_status)}; font-weight: bold;">{int(temperature_value) if temperature_value else 0}°</span> {get_trend_text(temperature_trend)}，全市场处于 <span style="color: {get_status_color(market_sentiment_status)}; font-weight: bold;">{get_status_text(market_sentiment_status)}</span> 状态。</span><br><br>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;"><tr style="background-color: #f1f8ff;">
<th style="padding: 6px; text-align: left; border: 1px solid #eee;">指标</th>
<th style="padding: 6px; text-align: center; border: 1px solid #eee;">数值</th>
<th style="padding: 6px; text-align: center; border: 1px solid #eee;">历史区间百分位</th>
</tr><tr>
<td style="padding: 6px; border: 1px solid #eee;">沪深市净率(PB)</td>
<td style="padding: 6px; text-align: center; border: 1px solid #eee;">{market_overview['pb']['ratio']:.2f}</td>
<td style="padding: 6px; text-align: center; border: 1px solid #eee;">{market_overview['pb']['range_percentile']:.2f}%</td>
</tr><tr>
<td style="padding: 6px; border: 1px solid #eee;">沪深市盈率(PE)</td>
<td style="padding: 6px; text-align: center; border: 1px solid #eee;">{market_overview['pe']['ratio']:.2f}</td>
<td style="padding: 6px; text-align: center; border: 1px solid #eee;">{market_overview['pe']['range_percentile']:.2f}%</td>
</tr><tr>
<td style="padding: 6px; border: 1px solid #eee;">全市场股息率</td>
<td style="padding: 6px; text-align: center; border: 1px solid #eee;">{market_overview.get('dividend_ratio', '-'):.2f}%</td>
<td style="padding: 6px; text-align: center; border: 1px solid #eee;">-</td>
</tr></table></section><section style="margin: 20px 0;"><br>

<h3 style="color: #333; font-weight: bold;">◆ 主要指数估值</h3><br>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;"><tr style="background-color: #f1f8ff;">
<th style="width: 50%; padding: 6px; text-align: left; border: 1px solid #eee;">指数名称</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">PE-TTM</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">指数温度</th>
</tr>{generate_index_rows(index_data)}</table><br>

<h3 style="color: #333; font-weight: bold;">◆ 主要行业估值</h3><br>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;"><tr style="background-color: #f1f8ff;">
<th style="width: 50%; padding: 6px; text-align: left; border: 1px solid #eee;">行业名称</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">PE-TTM</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">PB</th>
</tr>{generate_industry_rows(industry_valuation)}</table><br>

</section></section>{bottom_content}{bottom_signature_black13eard}
"""
            

            # 构建第二篇文章，市场热点及可转债市场分析
            cb_view = ConvertibleBondMarketDataView()
            response = cb_view.get(request)
            
            if response.status_code != 200:
                return response
            
            cb_data = response.data['data']


            title_1 = f"「PlanB」今日可转债数据（{formatted_date}）"
            digest_1 = f"「PlanB」每日财经新闻及可转债市场数据分享"
            thumb_media_id_1 = '_keMPIcYylD5UoO-1wwN4H_qSiejWTdRVoPLSuWZfnMX_j9iGLnL96E6aMCbQ0kC'
            head_img_url_covertablebonds = f"https://mmbiz.qpic.cn/sz_mmbiz_png/YPR2LvJic9CmqdXpcLmlFJ48h5lFxibXCibcox7AXFKERoyM2SupvB1Z9FzHnYXkDPavyrpXUKt5nlh3xyaz3aPQg/0?wx_fmt=png&from=appmsg"
            head_content = f"""<header style="background-image: url({head_img_url_covertablebonds}); background-size: cover; background-position: center; height: 400px;"></header>"""

            # 构建今日财经新闻
            content_news = f"""<h3 style="color: #333; font-weight: bold; margin: 0 0 10px 0;">◆ 今日财经新闻</h3>
            <p>{_news_raw}</p>"""
            
            # 构建可转债市场概况
            content_cb_market_overview = f"""<h3 style="color: #333; font-weight: bold; margin: 0 0 10px 0;">◆ 可转债市场概况</h3>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa; margin-top: 15px;"><tr style="background-color: #f1f8ff;">
<th style="padding: 6px; text-align: center; border: 1px solid #eee;">指标</th>
<th style="padding: 6px; text-align: center; border: 1px solid #eee;">数值</th></tr>
<tr><td style="padding: 6px; text-align: center; border: 1px solid #eee;">可转债指数</td><td style="padding: 6px; text-align: center; border: 1px solid #eee;">{cb_data['cb_index_data'][0]['price']:.2f} {cb_data['cb_index_data'][0]['trend']}</td></tr>
<tr><td style="padding: 6px; text-align: center; border: 1px solid #eee;">平均价格</td><td style="padding: 6px; text-align: center; border: 1px solid #eee;">{cb_data['cb_index_data'][0]['avg_price']:.2f}元</td></tr>
<tr><td style="padding: 6px; text-align: center; border: 1px solid #eee;">中位数价格</td><td style="padding: 6px; text-align: center; border: 1px solid #eee;">{cb_data['cb_index_data'][0]['mid_price']:.2f}元</td></tr>
<tr><td style="padding: 6px; text-align: center; border: 1px solid #eee;">平均溢价率</td><td style="padding: 6px; text-align: center; border: 1px solid #eee;">{cb_data['cb_index_data'][0]['avg_premium_rt']:.2f}%</td></tr>
<tr><td style="padding: 6px; text-align: center; border: 1px solid #eee;">到期收益率</td><td style="padding: 6px; text-align: center; border: 1px solid #eee;">{cb_data['cb_index_data'][0]['avg_ytm_rt']:.2f}%</td></tr>
</table>"""

            # 构建双低可转债
            content_table_doublelow = f"""<h3 style="color: #333; font-weight: bold; margin: 0 0 10px 0;">◆ 双低策略</h3>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;"><tr style="background-color: #f1f8ff;">
<th style="width: 50%; padding: 6px; text-align: left; border: 1px solid #eee;">转债名称</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">双低值</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">评级</th>
</tr>{generate_cb_doublelow_rows(cb_data['cb_doublelow_data'])}</table>"""
            
            # 构建小盘低价格可转债
            content_table_small_lowprice = f"""<h3 style="color: #333; font-weight: bold; margin: 0 0 10px 0;">◆ 小盘低价格策略</h3>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;"><tr style="background-color: #f1f8ff;">
<th style="width: 50%; padding: 6px; text-align: left; border: 1px solid #eee;">转债名称</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">价格</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">评级</th>
</tr>{generate_cb_small_lowprice_rows(cb_data['cb_smallsize_data'])}</table>"""
            
            # 构建小盘低溢价可转债
            content_table_small_lowpremium_rate = f"""<h3 style="color: #333; font-weight: bold; margin: 0 0 10px 0;">◆ 小盘低溢价策略</h3>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;"><tr style="background-color: #f1f8ff;">
<th style="width: 50%; padding: 6px; text-align: left; border: 1px solid #eee;">转债名称</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">溢价率</th>
<th style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">评级</th>
</tr>{generate_cb_small_lowpremium_rate_rows(cb_data['cb_smallsize_data'])}</table>"""

            # 构建已公告强赎可转债
            content_table_cb_redemption_public = f"""<h3 style="color: #333; font-weight: bold; margin: 0 0 10px 0;">◆ 已公告强赎</h3>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;"><tr style="background-color: #f1f8ff;">
<th style="width: 40%; padding: 6px; text-align: left; border: 1px solid #eee;">转债名称</th>
<th style="width: 20%; padding: 6px; text-align: center; border: 1px solid #eee;">强赎价</th>
<th style="width: 40%; padding: 6px; text-align: center; border: 1px solid #eee;">最后交易日</th>
</tr>{generate_cb_redemption_public_rows(cb_data['cb_redemption_bonds'])}</table>"""

            # 构建强赎倒计时可转债
            content_table_cb_redemption_countdown = f"""<h3 style="color: #333; font-weight: bold; margin: 0 0 10px 0;">◆ 强赎倒计时</h3>
<table style="width: 100%; border-collapse: collapse; background-color: #f8f9fa;"><tr style="background-color: #f1f8ff;">
<th style="width: 40%; padding: 6px; text-align: left; border: 1px solid #eee;">转债名称</th>
<th style="width: 20%; padding: 6px; text-align: center; border: 1px solid #eee;">强赎触发价</th>
<th style="width: 40%; padding: 6px; text-align: center; border: 1px solid #eee;">进度</th>
</tr>{generate_cb_redemption_countdown_rows(cb_data['cb_redemption_bonds'])}</table>"""

            content_1 = f"""
<section style="font-size: 14px; line-height: 2; box-sizing: border-box; font-style: normal; font-weight: 400; text-align: justify; visibility: visible;">
<p>{content_cb_market_overview}</p>
<p>{content_table_doublelow}</p>
<p>{content_table_small_lowprice}</p>
<p>{content_table_small_lowpremium_rate}</p>
<p>{content_table_cb_redemption_public}</p>
<p>{content_table_cb_redemption_countdown}</p>

</p></section>

{bottom_content}{bottom_signature_black13eard}
"""


            # 构建文章数据数组
            articles = {
                'titles': [
                    title_1,
                    title_0
                ],
                'contents': [
                    content_1,
                    content_0
                ],
                'digests': [
                    digest_1,
                    digest_0
                ],
                'thumb_media_ids': [
                    thumb_media_id_1,
                    thumb_media_id_0
                ]
            }

            # 同步到微信公众号草稿箱
            success, result = sync_to_wechat_draft(
                # wx_account='fire_qinglv',
                wx_account='fire_black13eard',
                articles=articles
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
        'Low': '#52c41a',     # 绿色 - 低估
        'Unknown': '#8c8c8c'   # 灰色 - 未知状态
    }
    return color_map.get(status, '#ff9900')  # 默认返回橙色

def get_status_text(status):
    """根据状态返回对应的中文描述"""
    text_map = {
        'High': '高估',
        'Medium': '中估',
        'Low': '低估',
        'Unknown': '未知'
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
    for data in index_data:
        if 'name' in data and 'code' in data:
            row = f"""<tr>
<td style="width: 50%; padding: 6px; border: 1px solid #eee;">{data['name']}({data['code']})</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{data['pe_ttm_ratio']:.2f}</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{int(data['percentile'])}° {data.get('trend', '-')}</td>
</tr>
"""
            rows.append(row)
    return ''.join(rows)

def generate_industry_rows(industry_data):
    """生成行业行数据的辅助函数"""
    rows = []
    for data in industry_data:
        # 安全地获取数值
        pb_ratio = data.get('pb_ratio')
        pe_ttm_ratio = data.get('pe_ttm_ratio')
        
        # 格式化显示，如果是 None 则显示 '-'
        pb_display = f"{pb_ratio:.2f}" if pb_ratio is not None else '-'
        pe_display = f"{pe_ttm_ratio:.2f}" if pe_ttm_ratio is not None else '-'

        row = f"""<tr>
<td style="width: 50%; padding: 6px; border: 1px solid #eee;">{data.get('name', '-')}</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{pe_display}</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{pb_display}</td>
</tr>
"""
        rows.append(row)
    return ''.join(rows)


def generate_cb_doublelow_rows(index_data):
    """生成可转债行数据的辅助函数，只返回前10个数据"""
    
    rows = []
    # 只取前10个数据
    for data in index_data[:10]:
        if 'name' in data and 'code' in data:
            row = f"""<tr>
<td style="width: 50%; padding: 6px; border: 1px solid #eee;">{data['name']}({data['code']})</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{int(data['double_low'])}</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{data.get('bond_rating', '-')}</td>
</tr>
"""
            rows.append(row)
    return ''.join(rows)

def generate_cb_small_lowprice_rows(index_data):
    """生成可转债行数据的辅助函数，按价格排序并返回前10个最低价数据"""
    rows = []
    # 按照价格(close)排序
    sorted_data = sorted(index_data, key=lambda x: x['close'])
    # 只取排序后的前10个数据
    for data in sorted_data[:10]:
        if 'name' in data and 'code' in data:
            row = f"""<tr>
<td style="width: 50%; padding: 6px; border: 1px solid #eee;">{data['name']}({data['code']})</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{int(data['close'])}</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{data.get('bond_rating', '-')}</td>
</tr>"""
            rows.append(row)
    return ''.join(rows)


def generate_cb_small_lowpremium_rate_rows(index_data):
    """生成可转债行数据的辅助函数，按溢价率排序并返回前10个最低价数据"""
    
    rows = []
    # 按照溢价率(premium_rate)排序
    sorted_data = sorted(index_data, key=lambda x: x['premium_rate'])
    # 只取排序后的前10个数据
    for data in sorted_data[:10]:
        if 'name' in data and 'code' in data:
            row = f"""<tr>
<td style="width: 50%; padding: 6px; border: 1px solid #eee;">{data['name']}({data['code']})</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{data['premium_rate']:.2f}%</td>
<td style="width: 25%; padding: 6px; text-align: center; border: 1px solid #eee;">{data.get('bond_rating', '-')}</td>
</tr>
"""
            rows.append(row)
    return ''.join(rows)


def generate_cb_redemption_public_rows(index_data):
    """生成可转债行数据的辅助函数，按最后交易日排序并返回数据"""
    
    rows = []
    # 先筛选出符合条件的数据
    filtered_data = [
        data for data in index_data 
        if 'name' in data and 'code' in data 
        and data['is_callable'] in ['已公告强赎', '公告要强赎']
    ]
    
    # 对筛选后的数据按最后交易日排序
    # 将None值放在最后
    sorted_data = sorted(
        filtered_data, 
        key=lambda x: (x['last_trading_date'] is None, x['last_trading_date'])
    )
    
    # 使用排序后的数据生成行
    for data in sorted_data:
        # 处理 redemption_price 可能为 None 的情况
        redemption_price = data.get('redemption_price')
        if redemption_price is not None:
            redemption_price = f"{redemption_price:.2f}"
        else:
            redemption_price = '-'

        # 处理 last_trading_date 可能为 None 的情况
        last_trading_date = data.get('last_trading_date', '-')
        if last_trading_date == None:
            last_trading_date = '-'

        row = f"""<tr>
<td style="width: 40%; padding: 6px; border: 1px solid #eee;">{data['name']}({data['code']})</td>
<td style="width: 20%; padding: 6px; text-align: center; border: 1px solid #eee;">{redemption_price}</td>
<td style="width: 40%; padding: 6px; text-align: center; border: 1px solid #eee;">{last_trading_date}</td>
</tr>
"""
        rows.append(row)
    return ''.join(rows)


def generate_cb_redemption_countdown_rows(index_data):
    """生成可转债行数据的辅助函数，按进度排序并返回数据"""
    
    rows = []
    
    # 先筛选出符合条件的数据
    filtered_data = [
        data for data in index_data 
        if 'name' in data and 'code' in data 
        and data['is_callable'] not in ['已公告强赎', '公告要强赎']
    ]
    
    # 对筛选后的数据按进度排序，将None值放在最后
    sorted_data = sorted(
        filtered_data, 
        key=lambda x: (x['display_countdown'] is None, x.get('display_countdown', ''))
    )
    
    # 使用排序后的数据生成行
    for data in sorted_data:
        # 安全地获取 redemption_trigger_price，如果是 None 则显示 '-'
        trigger_price = data.get('redemption_trigger_price')
        trigger_price_display = f"{trigger_price:.2f}" if trigger_price is not None else '-'
        
        row = f"""<tr>
<td style="width: 40%; padding: 6px; border: 1px solid #eee;">{data['name']}({data['code']})</td>
<td style="width: 20%; padding: 6px; text-align: center; border: 1px solid #eee;">{trigger_price_display}</td>
<td style="width: 40%; padding: 6px; text-align: center; border: 1px solid #eee;">{data.get('display_countdown', '-')}</td>
</tr>
"""
        rows.append(row)
    return ''.join(rows)

def generate_news():
    """生成新闻内容"""
    kimi_chat = KimiChatView()
    request = Request(HttpRequest())
    request._full_data = {
        "messages": [
            {
                "role": "system",
                "content": "你是 Kimi，由 Moonshot AI 提供的人工智能助手：财经新闻评论员，你擅长联网阅读、收集、整理每日的经济新闻、财经新闻和国际新闻，并进行总结。你会为用户提供安全，有帮助，准确的回答。同时，你会拒绝一切涉及恐怖主义，种族歧视，黄色暴力等问题的回答。Moonshot AI 为专有名词，不可翻译成其他语言。总结格式为：【NO.x {新闻标题}】：{新闻内容} <br><span style=\"font-size: 14px; color: #8B0000; font-weight: bold;\">【点评】{点评内容}</span><br>。"
            },
            {
                "role": "user",
                "content": f"无需任何开场白和结束语，直接按照格式生成今天({datetime.now().strftime('%Y-%m-%d')})的财经新闻要点。新闻来源可参考如下网站：http://www.cnstock.com/、http://www.eastmoney.com/、https://finance.sina.com.cn/、https://www.cctv.com/、https://www.nfapp.southcn.com/、https://money.163.com/、https://www.yicai.com/、https://www.nbd.com.cn/、https://www.ce.cn/、https://www.cls.cn/、https://www.investing.com/、https://www.nbd.com.cn/、https://www.ce.cn/、https://www.cls.cn/、https://www.investing.com/、https://www.cls.cn/、https://www.investing.com/。"
            },
            {
                "role": "assistant",
                "content": " ",
                "partial": True
            }
        ]
    }
    
    response = kimi_chat.post(request)
    # print(response.data['content'])
    return response.data['content'] if response.status_code == 200 else "今日暂无重要财经新闻。"

def get_cached_news():
    """获取缓存的新闻内容，如果没有则生成新的"""
    cache_key = f'daily_news_{datetime.now().strftime("%Y-%m-%d")}'
    cached_news = cache.get(cache_key)
    
    #if cached_news:
        # print("cached_news:", cached_news)
    #    return cached_news
        
    # 如果没有缓存，生成新内容
    try:
        news_content = generate_news()
        # 缓存一天
        cache.set(cache_key, news_content, 24*60*60)
        return news_content
    except Exception as e:
        logger.error(f"Error generating news: {str(e)}")
        return "今日暂无重要财经新闻。"

def generate_news_content(subject):
    """优化后的新闻生成函数"""
    if subject == 'news':
        return get_cached_news()
    # ... 其他逻辑保持不变



class MarketValuationSyncView2(APIView):
    def get(self, request):
        try:
            # 设置更长的超时时间
            request.META['HTTP_TIMEOUT'] = '300'  # 5分钟超时
            # 构建第一篇文章：市场整体估值。从 fire_100UpPlan 获取市场估值数据
            # 构建文章首位内容
            bottom_content = f"""<p style="text-align: right;"><span style="font-size: 12px; color: #999; font-style: italic;">**以上来源于公开数据，仅供参考**</span></p>"""
            bottom_signature_black13eard = f"""<section style="font-size: 14px; line-height: 2; color: rgb(62, 62, 62); visibility: visible;"><p style="visibility: visible;"><br style="visibility: visible;"></p><section style="margin-top: 0.5em; margin-bottom: 0.5em; visibility: visible;"><section style="display: inline-block; vertical-align: top; margin-top: 0.5em; margin-bottom: 0.5em; width: 100%; visibility: visible;"><section style="visibility: visible;"><section style="box-shadow: rgba(159, 160, 160, 0.5) 0px 0px 10px; padding: 10px; display: inline-block; vertical-align: top; visibility: visible;"><section style="box-shadow: rgba(0, 0, 0, 0.29) 0px 0px 10px inset; padding: 7px; visibility: visible;"><section style="text-align: center; line-height: 0; visibility: visible;"><section style="vertical-align: middle; display: inline-block; line-height: 0; visibility: visible;"><img class="rich_pages wxw-img" data-imgfileid="100000125" data-ratio="0.75" data-s="300,640" data-src="https://mmbiz.qpic.cn/mmbiz_jpg/Y1ciargAZx1CoYhEz7FKDjJtPCXgLXOqI2wEGlfrjYeA7qKic9e3DmfYo9ic6yUK7GicPiaKyFLrN13DvuIDCJianG9A/640?wx_fmt=jpeg&amp;from=appmsg" data-type="jpeg" data-w="1080" style="vertical-align: middle; width: 643px !important; height: auto !important; visibility: visible !important;" data-original-style="vertical-align: middle;width: 100%;" data-index="1" src="https://mmbiz.qpic.cn/mmbiz_jpg/Y1ciargAZx1CoYhEz7FKDjJtPCXgLXOqI2wEGlfrjYeA7qKic9e3DmfYo9ic6yUK7GicPiaKyFLrN13DvuIDCJianG9A/640?wx_fmt=jpeg&amp;from=appmsg&amp;tp=webp&amp;wxfrom=5&amp;wx_lazy=1&amp;wx_co=1" _width="100%" crossorigin="anonymous" alt="图片" data-fail="0"></section></section><section style="clear: both; visibility: visible;"><svg viewBox="0 0 1 1" style="float: left; line-height: 0; width: 0px; vertical-align: top; visibility: visible;"></svg></section></section></section></section><section style="padding-top: 5px; padding-bottom: 5px; visibility: visible;"><section style="font-size: 8px; text-align: right; visibility: visible;"><p style="visibility: visible;"><br style="visibility: visible;"></p></section></section><section style="color: rgba(0, 0, 0, 0.61);text-align: right;"><p>蜀之鄙有二僧，其一贫，其一富。</p><p>贫者语于富者曰：「吾欲之南海，何如？」</p><p>富者曰：「子何恃而往？」</p><p>曰：「吾一瓶一钵足矣。」</p><p>富者曰：「吾数年来欲买舟而下，犹未能也。</p><p>子何持而往！」</p><p>越明年，贫者自南海还。</p><p><br></p></section></section></section><section class="mp_profile_iframe_wrp"><mp-common-profile class="js_uneditable custom_select_card mp_profile_iframe mp_common_widget js_wx_tap_highlight" data-pluginname="mpprofile" data-id="Mzk1NzM0OTAzMw==" data-headimg="http://mmbiz.qpic.cn/mmbiz_png/Y1ciargAZx1CcpZhWPeQd098IAbG69FXXqHczLt8mMW5KSnfBmVrq4qrIsxWPLvlpLnrn0wO0KVmicWtTDj4YJrQ/300?wx_fmt=png&amp;wxfrom=19" data-nickname="FIRE轻旅" data-alias="fireqinglv" data-signature="践行 F.I.R.E 理念，倡导简约自由的轻旅生活。" data-from="0" data-is_biz_ban="0" data-service_type="1" data-origin_num="0" data-isban="0" data-biz_account_status="0" data-index="0"></mp-common-profile></section></section>"""

            # 构建第二篇文章，市场热点及可转债市场分析

            title_1 = f"可转债今日市场分析"
            digest_1 = f"又到了一年一度的个人所得税申报时间了。"
            thumb_media_id_1 = '_keMPIcYylD5UoO-1wwN4H_qSiejWTdRVoPLSuWZfnMX_j9iGLnL96E6aMCbQ0kC'
            head_img_url = f"http://mmbiz.qpic.cn/sz_mmbiz_jpg/YPR2LvJic9Cm3nBHz8v71c9DrI24azTHRm8ZhgwwsIfnvQLfDYPO9BrYlzkpgGzqjFoCuLTOA2JR9LopS7TBglg/0?from=appmsg"
            head_content = f"""<header style="background-image: url({head_img_url}); background-size: cover; background-position: center; height: 200px;"></header>"""

            # 构建今日财经新闻
            content_news = f"""<h3 style="color: #333; font-weight: bold; margin: 0 0 10px 0;">◆ 今日财经新闻</h3>
            <p>{_news_raw}</p>"""
            
            

            content_1 = f"""
{head_content}
<section style="font-size: 14px; line-height: 2; box-sizing: border-box; font-style: normal; font-weight: 400; text-align: justify; visibility: visible;">
<p>{content_news}</p>

<p><h3 style="color: #333; font-weight: bold;">◆ 个人养老金</h3>
按照发布的个人养老金政策，我们可以在银行设立一个专门用于存放个人养老金的账户。账户里的钱只进不出，直至退休，里面的钱可以用于自主投资。我们每年缴纳个人养老金的上限为 12000 元，可以自主决定缴多少，本年度内既可以一次性缴也可以分次缴。同时，这部分钱能够享受一定的税收优惠。按照每年缴纳 12000 元，按照每个人的缴税档位，最高可以退 5400 元。</p>
<p><br>
</p></section>

{bottom_content}{bottom_signature_black13eard}
"""


            # 构建文章数据数组
            articles = {
                'titles': [
                    title_1
                ],
                'contents': [
                    content_1
                ],
                'digests': [
                    digest_1
                ],
                'thumb_media_ids': [
                    thumb_media_id_1
                ]
            }

            # 同步到微信公众号草稿箱
            success, result = sync_to_wechat_draft(
                wx_account='fire_black13eard',
                articles=articles
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
            logger.error(f"Error in MarketValuationSyncView2: {str(e)}")
            return Response({
                'code': 1,
                'error': str(e),
                'message': 'Failed'
            }, status=500)