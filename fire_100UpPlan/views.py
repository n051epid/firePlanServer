from rest_framework import viewsets, generics
from rest_framework.views import APIView
from rest_framework.response import Response
from .models import Membership, MembershipType
from .serializers import MembershipSerializer
from rest_framework.permissions import IsAuthenticated
from django.contrib.auth.models import User
from .permissions import MembershipPermission
from rest_framework import status
from django.utils import timezone
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.tokens import RefreshToken
import requests
from django.conf import settings
from .utils import verify_paypal_webhook
import paypalrestsdk
from .models import MembershipType, Membership, MembershipPurchase
from rest_framework import generics
from .serializers import MembershipTypeSerializer, CustomTokenObtainPairSerializer
from django.http import HttpResponse
import json
import logging
from django.db import transaction
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.urls import reverse
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_encode, urlsafe_base64_decode
from django.contrib.auth.tokens import default_token_generator
from .tasks import send_notification_email
from django.shortcuts import render
from django.utils.encoding import force_str
from allauth.socialaccount.providers.google.views import GoogleOAuth2Adapter
from allauth.socialaccount.providers.oauth2.client import OAuth2Client
from dj_rest_auth.registration.views import SocialLoginView
from django.middleware.csrf import get_token
from django.http import JsonResponse
import traceback
from django.views.decorators.csrf import ensure_csrf_cookie
from allauth.socialaccount.models import SocialAccount
from rest_framework_simplejwt.views import TokenObtainPairView
from .serializers import CustomTokenObtainPairSerializer
from .serializers import UserRegistrationSerializer



logger = logging.getLogger(__name__)

# 配置 PayPal SDK
paypalrestsdk.configure({
    "mode": settings.PAYPAL_MODE,  # 沙盒或生产环境
    "client_id": settings.PAYPAL_CLIENT_ID,
    "client_secret": settings.PAYPAL_CLIENT_SECRET,
    "api_base": settings.PAYPAL_API_BASE
})

def get_csrf_token(request):
    return JsonResponse({'csrfToken': get_token(request)})


class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer

    def post(self, request, *args, **kwargs):
        response = super().post(request, *args, **kwargs)
        return response


class UserAuthCodesView(APIView):
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        try:
            # 这里可以根据实际需求从数据库或其他地方获取授权码
            # 示例数据
            auth_codes = [
                "AC_1000001",
                "AC_1000002"
            ]
            
            response = {
                "code": 0,
                "data": auth_codes,
                "error": None,
                "message": "ok"
            }
            return Response(response)
            
        except Exception as e:
            error_response = {
                "code": 1,
                "data": None,
                "error": str(e),
                "message": "Get Auth Codes Failed"
            }
            return Response(error_response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UserLogoutView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            # 获取用户的刷新令牌
            refresh_token = request.data.get('refresh_token')
            if refresh_token:
                token = RefreshToken(refresh_token)
                # 将令牌加入黑名单
                token.blacklist()
            
            response = {
                "code": 0,
                "data": None,
                "error": None,
                "message": "Logout Successful"
            }
            return Response(response)
            
        except Exception as e:
            error_response = {
                "code": 1,
                "data": None,
                "error": str(e),
                "message": "Logout Failed"
            }
            return Response(error_response, status=status.HTTP_400_BAD_REQUEST)


class RegisterView(APIView):
    authentication_classes = []  # 禁用认证
    permission_classes = []  # 禁用限检查

    def post(self, request):
        username = request.data.get('username')
        email = request.data.get('username')
        password = request.data.get('password')
        confirmPassword = request.data.get('confirmPassword')

        if not username or not password:
            return Response({
                "code": 1,
                "data": None,
                "error": "Username and Password are Required Fields.",
                "message": "Registration Failed"
            }, status=status.HTTP_400_BAD_REQUEST)

        if password != confirmPassword:
            return Response({
                "code": 1,
                "data": None,
                "error": "Password and Confirm Password do not match.",
                "message": "Registration Failed"
            }, status=status.HTTP_400_BAD_REQUEST)

        try:
            serializer = UserRegistrationSerializer(data={
                'username': username,
                'email': email,
                'password': password
            })
            if not serializer.is_valid():
                # 获取第一个错误信息
                error_msg = next(iter(serializer.errors.values()))[0] if serializer.errors else "Validation failed"
                return Response({
                    "code": 1,
                    "data": None,
                    "error": str(error_msg),  # 使用具体的错误信息
                    "message": "Registration Failed"
                }, status=status.HTTP_400_BAD_REQUEST)
                
            user = serializer.save()

            # 创建Free User会员资格
            free_user_type = MembershipType.objects.get(name="Free User")
            Membership.objects.create(user=user, membership_type=free_user_type)

            # 生成激活令牌
            active_token = default_token_generator.make_token(user)
            uid = urlsafe_base64_encode(force_bytes(user.pk))
            activation_link = request.build_absolute_uri(
                reverse('activate_account', kwargs={'uidb64': uid, 'token': active_token})
            )

            # 异步发送激活邮件
            subject = 'GuizhenIntel activation'
            message = f'Your account has been successfully created. Please click the link below to activate your account,and obtain GuizhenIntel Trial Plan:\n\n{activation_link}'
            from_email = settings.DEFAULT_FROM_EMAIL
            recipient_list = [email]
            bcc_list = [settings.BCC_EMAIL]
            send_notification_email.delay(subject, message, from_email, recipient_list, bcc_list)

            # 生成令牌
            refresh = RefreshToken.for_user(user)
            
            return Response({
                "code": 0,
                "data": {
                    "username": serializer.data['username'],
                    # "email": serializer.data['email'],
                    "accessToken": str(refresh.access_token),
                    "refreshToken": str(refresh)
                },
                "message": "Signup Successful, activation email has been sent.",
                "error": None
            }, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            return Response({
                "code": 1,
                "data": None,
                "error": str(e),
                "message": "Registration Failed"
            }, status=status.HTTP_400_BAD_REQUEST)

# 新增激活账户的视图
class ActivateAccountView(APIView):
    def get(self, request, uidb64, token):
        try:
            uid = force_str(urlsafe_base64_decode(uidb64))
            user = User.objects.get(pk=uid)
        except (TypeError, ValueError, OverflowError, User.DoesNotExist):
            user = None

        context = {}
        if user is not None and default_token_generator.check_token(user, token):
            if  user.is_active:
                # user.is_active = True
                # user.save()

                # 升级为试用会员资格
                trial_type = MembershipType.objects.get(name="Trial")
                membership, created = Membership.objects.get_or_create(user=user)
                
                # 设置试用期开始时间和结束时间
                current_time = timezone.now()
                membership.membership_type = trial_type
                membership.start_date = current_time
                membership.end_date = current_time + timezone.timedelta(days=trial_type.duration_days)
                membership.is_active = True
                membership.save()

                context['status'] = 'activated_trial'
            else:
                context['status'] = 'already_activated'
        else:
            context['status'] = 'invalid_link'

        return render(request, 'account_activation.html', context)


class MembershipViewSet(viewsets.ModelViewSet):
    queryset = Membership.objects.all()
    serializer_class = MembershipSerializer
    permission_classes = [IsAuthenticated]

class MembershipTypeListView(generics.ListAPIView):
    serializer_class = MembershipTypeSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        # queryset = MembershipType.objects.filter(id__gt=1)
        queryset = MembershipType.objects.filter(id=2)
        if not queryset.exists():
            return MembershipType.objects.none()
        return queryset

class UserInfoView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = request.user
        try:
            membership = Membership.objects.get(user=user, is_active=True)
            membership_type = membership.membership_type.name
            end_date = membership.end_date
            is_active = end_date > timezone.now() if end_date else False
        except Membership.DoesNotExist:
            membership_type = "Free User"
            end_date = None
            is_active = False

        try:
            response = {
                "code": 0,
                "data": {
                    "username": user.username,
                    "realName": user.username.split('@')[0],
                    "roles": list(user.groups.all().values_list('name', flat=True)),
                    "membership_type": membership_type,
                    "membership_end_date": end_date.isoformat() if end_date else None,
                    "is_membership_active": is_active
                },
                "error": None,
                "message": "Success"
            }
            return Response(response)
        except Exception as e:
            error_response = {
                "code": 1,
                "data": None,
                "error": str(e),
                "message": "Get User Info Failed"
            }
            return Response(error_response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# 会员功能视图
class BasicFeatureView(APIView):
    permission_classes = [MembershipPermission(allowed_memberships=['Basic Monthly'])]
    def get(self, request):
        return Response({"message": "This is a basic feature"})

class AdvancedFeatureView(APIView):
    permission_classes = [MembershipPermission(allowed_memberships=['Premium Monthly'])]
    def get(self, request):
        return Response({"message": "This is an advanced feature"})

# minimax api
class MinimaxT2AView(APIView):
    name = "ParroT T2A Basic" # 这将被用作链接文本
    permission_classes = [IsAuthenticated, MembershipPermission]
    
    def get_permissions(self):
        return [IsAuthenticated(), MembershipPermission(allowed_memberships=['Trial', 'Basic Monthly', 'Premium Monthly'])]
    
    def post(self, request):
        # 从 Django 设置中获取 MiniMax API 凭证
        group_id = settings.MINIMAX_GROUP_ID
        api_key = settings.MINIMAX_API_KEY

        # 构建 MiniMax API 请求 URL
        url = f'https://api.minimax.chat/v1/t2a_v2?GroupId={group_id}'

        # 设置请求头
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }

        # 从请求体中获取必要参数
        text = request.data.get('text')
        voice_id = request.data.get('voice_id', 'male-qn-qingse')
        language_boost = request.data.get('language_boost', 'auto')

        # 检查必要参数
        if not text:
            return Response({'error': 'Text is required'}, status=status.HTTP_400_BAD_REQUEST)

        # 构建完整的请求数据
        data = {
            "model": "speech-01-turbo",
            "text": text,
            "stream": False,
            "voice_setting": {
                "voice_id": voice_id,
                "speed": 1,
                "vol": 10,
                "pitch": 0
            },
            "pronunciation_dict": {
                "tone": []
            },
            "audio_setting": {
                "sample_rate": 32000,
                "bitrate": 128000,
                "format": "mp3",
                "channel": 2
            },
            "language_boost": language_boost
        }

        try:
            # 发送请到 MiniMax API
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()  # 如果请求失败,会抛出异常

            # 返回 MiniMax API 的响应
            return Response(response.json(), status=status.HTTP_200_OK)

        except requests.RequestException as e:
            # 处理请求异常
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CreatePayPalOrderView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        membership_type_id = request.data.get('membership_type_id')
        try:
            membership_type = MembershipType.objects.get(id=membership_type_id)
        except MembershipType.DoesNotExist:
            return Response({"error": "Invalid membership type"}, status=status.HTTP_400_BAD_REQUEST)

        payment = paypalrestsdk.Payment({
            "intent": "sale",
            "payer": {
                "payment_method": "paypal"
            },
            "redirect_urls": {
                "return_url": f"https://{settings.ALLOWED_HOST}/v1/payment/success/",
                "cancel_url": f"https://{settings.ALLOWED_HOST}/v1/payment/cancel/"
            },
            "transactions": [{
                "item_list": {
                    "items": [{
                        "name": membership_type.name,
                        "sku": f"membership-{membership_type.id}",
                        "price": str(membership_type.price),
                        "currency": "USD",
                        "quantity": 1
                    }]
                },
                "amount": {
                    "total": str(membership_type.price),
                    "currency": "USD"
                },
                "description": f"purchase {membership_type.name} membership"
            }]
        })

        if payment.create():
            purchase = MembershipPurchase.objects.create(
                user=request.user,
                membership_type=membership_type,
                payment_status='PENDING',
                transaction_id=payment.id,
                amount_paid=membership_type.price  # 设置 amount_paid
            )

            # 找到批准 URL
            for link in payment.links:
                if link.rel == "approval_url":
                    approval_url = link.href
                    return Response({
                        "approval_url": approval_url,
                        "payment_id": payment.id,
                        # "purchase_id": purchase.id
                    })
        else:
            return Response({"error": payment.error}, status=status.HTTP_400_BAD_REQUEST)



# 然后在两个视图中使用这些方法
class CapturePayPalOrderView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        payment_id = request.data.get('paymentId')
        payer_id = request.data.get('payerId')

        try:
            with transaction.atomic():
                purchase = MembershipPurchase.objects.select_for_update().get(transaction_id=payment_id)
                
                if purchase.payment_status == 'COMPLETED':
                    return Response({"status": "Payment already processed"}, status=status.HTTP_200_OK)
                
                payment = paypalrestsdk.Payment.find(payment_id)

                if payment.state == 'approved':
                    # 支付已经成，更新本地状态
                    if purchase.payment_status != 'COMPLETED':
                        purchase.payment_status = 'COMPLETED'
                        purchase.save()
                        membership = update_membership(purchase.user, purchase.membership_type)
                    
                    return Response({
                        "status": "Payment already completed",
                        # "purchase_id": purchase.id,
                        "transaction_id": payment_id,
                        "purchased_membership": {
                            "purchased_membership_type": purchase.membership_type.name,
                            "purchased_start_date": purchase.purchase_date,
                        },
                        # 返回当前正在生效态的会员信息
                        "membership":{
                            # "id": membership.id,
                            "current_membership_type": membership.membership_type.name,
                            "start_date": membership.start_date,
                            "end_date": membership.end_date,
                            "is_active": membership.is_active
                        }
                    }, status=status.HTTP_200_OK)
                
                if payment.execute({"payer_id": payer_id}):
                    purchase.payment_status = 'COMPLETED'
                    purchase.save()
                    membership = update_membership(purchase.user, purchase.membership_type)
                    logger.info(f"Payment completed for purchase: {purchase.id}")
                    return Response({
                        "status": "Payment completed",
                        # "purchase_id": purchase.id,
                        # 返回当前正在生效状态的会员信息
                        "membership":{
                            "membership_type": membership.membership_type.name,
                            "start_date": membership.start_date,
                            "end_date": membership.end_date,
                            "is_active": membership.is_active
                        }
                    }, status=status.HTTP_200_OK)
                else:
                    logger.error(f"Payment execution failed: {payment.error}")
                    return Response({
                        "error": "Payment execution failed",
                        "payment_status": payment.state,
                        "details": payment.error
                    }, status=status.HTTP_400_BAD_REQUEST)
        except MembershipPurchase.DoesNotExist:
            logger.error(f"Invalid payment ID: {payment_id}")
            return Response({"error": "Invalid payment ID"}, status=status.HTTP_404_NOT_FOUND)
        except paypalrestsdk.ResourceNotFound:
            logger.error(f"Payment not found on PayPal: {payment_id}")
            return Response({"error": "Payment not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            logger.exception(f"Error capturing payment: {payment_id}")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class PayPalWebhookView(APIView):
    def post(self, request):
        logger.info("Received PayPal webhook")
        logger.info(f"Request headers: {request.headers}")
        logger.info(f"Request body: {request.body.decode('utf-8')}")

        try:
            # 解码 request.body
            body = request.body.decode('utf-8')
            payload = json.loads(body)
            
            logger.info(f"Parsed payload: {payload}")

            # 验证 webhook
            if not verify_paypal_webhook(
                request.headers.get('Paypal-Transmission-Id'),
                request.headers.get('Paypal-Transmission-Time'),
                request.headers.get('Paypal-Transmission-Sig'),
                body
            ):
                logger.error("Invalid webhook")
                return HttpResponse("Invalid webhook", status=400)

            logger.info("Webhook verified successfully")

            event_type = payload.get('event_type')
            logger.info(f"PayPal webhook event type: {event_type}")

            if event_type == 'PAYMENTS.PAYMENT.CREATED':
                return self.handle_payment_created(payload)
            elif event_type == 'PAYMENT.CAPTURE.COMPLETED':
                return self.handle_payment_completed(payload)
            else:
                logger.warning(f"Unhandled event type: {event_type}")
                return HttpResponse("Unhandled event type", status=200)

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON payload")
            return HttpResponse("Invalid JSON", status=400)
        except Exception as e:
            logger.error(f"Unexpected error in webhook processing: {str(e)}")
            return HttpResponse("Internal server error", status=500)

    def handle_payment_created(self, payload):
        logger.info("Handling PAYMENTS.PAYMENT.CREATED event")
        # 处理支付创建逻辑
        return HttpResponse("Payment created event processed", status=200)

    def handle_payment_completed(self, payload):
        logger.info("Handling PAYMENT.CAPTURE.COMPLETED event")
        # 处理支付完成逻辑
        return HttpResponse("Payment completed event processed", status=200)


def update_membership(user, new_membership_type):
    try:
        membership = Membership.objects.get(user=user, is_active=True)
        current_time = timezone.now()

        if membership.membership_type.name == "Free User":
            membership.end_date = current_time
            membership.membership_type = new_membership_type
            membership.start_date = current_time
            membership.end_date = current_time + timezone.timedelta(days=new_membership_type.duration_days)
            logger.info(f"Updated membership for user {user.id}: {membership.id}")
        

        if membership.membership_type == new_membership_type:
            # 新购买的会员类型和原有的一致
            if membership.end_date and membership.end_date > current_time:
                # 原有会员未过期
                membership.end_date += timezone.timedelta(days=new_membership_type.duration_days)
            else:
                # 原有会员已过期
                membership.start_date = current_time
                membership.end_date = current_time + timezone.timedelta(days=new_membership_type.duration_days)
        else:
            # 新购买的会员和原有的不一致
            if membership.end_date and membership.end_date > current_time:
                # 原有会员未过期
                new_end_date = membership.end_date + timezone.timedelta(days=new_membership_type.duration_days)
                Membership.objects.create(
                    user=user,
                    membership_type=new_membership_type,
                    start_date=membership.end_date,
                    end_date=new_end_date,
                    is_active=False
                )
            else:
                # 原有会员已过期
                membership.membership_type = new_membership_type
                membership.start_date = current_time
                membership.end_date = current_time + timezone.timedelta(days=new_membership_type.duration_days)

        membership.save()
        logger.info(f"Updated membership for user {user.id}: {membership.id}")
    except Membership.DoesNotExist:
        # 用户没有现有的会员记录，创建新的会员记录
        membership = Membership.objects.create(
            user=user,
            membership_type=new_membership_type,
            start_date=timezone.now(),
            end_date=timezone.now() + timezone.timedelta(days=new_membership_type.duration_days),
            is_active=True
        )
        logger.info(f"Created new membership for user {user.id}")
    return membership


class CheckPaymentStatusView(APIView):
    def get(self, request, payment_id):
        try:
            payment = paypalrestsdk.Payment.find(payment_id)
            if payment.state == 'approved':
                payer_id = payment.payer.payer_info.payer_id
                return Response({'status': 'server_approved', 'state': payment.state, 'payment_id': payment_id, 'payerId': payer_id})
            elif payment.state == 'failed':
                return Response({'status': 'server_failed', 'state': payment.state, 'payment_id': payment_id})
            else:
                return Response({'status': 'server_pending', 'state': payment.state, 'payment_id': payment_id})
        except paypalrestsdk.ResourceNotFound:
            return Response({'error': 'Payment not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class PurchaseHistoryView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        purchases = MembershipPurchase.objects.filter(user=request.user).order_by('-purchase_date')
        serializer = MembershipPurchaseSerializer(purchases, many=True)
        return Response(serializer.data)

class PaymentSuccessView(APIView):
    def get(self, request):
        payment_id = request.GET.get('paymentId')
        payer_id = request.GET.get('PayerID')
        
        try:
            payment = paypalrestsdk.Payment.find(payment_id)
            if payment.execute({"payer_id": payer_id}):
                # 支付成功，重定向到成功页面
                return render(request, 'payment_success.html')
            else:
                # 支付执行失败，返回错误信息
                return render(request, 'payment_error.html', {'error': "payment failed"}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            # 处理异常情况
            return render(request, 'payment_error.html', {'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class PaymentCancelView(APIView):
    def get(self, request):
        # 更新订单状态为取消，待更新一个支付取消页面
        return render(request, 'payment_cancel.html')

@method_decorator(ensure_csrf_cookie, name='dispatch')
class GoogleLogin(SocialLoginView):
    adapter_class = GoogleOAuth2Adapter
    callback_url = "http://127.0.0.1:8000/accounts/google/login/callback/"
    client_class = OAuth2Client

    def get_serializer(self, *args, **kwargs):
        serializer_class = self.get_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        return serializer_class(*args, **kwargs)

    def post(self, request, *args, **kwargs):
        try:
            logger.info(f"Received Google login request: {request.data}")
            response = super().post(request, *args, **kwargs)
            
            user = self.user
            if user:
                social_account = SocialAccount.objects.filter(user=user, provider='google').first()
                if social_account:
                    google_account_id = social_account.uid
                    is_new_user = not Membership.objects.filter(user__socialaccount__uid=google_account_id).exists()
                    if is_new_user:
                        self.register_new_user(user, social_account.extra_data)
            
            # 生成 JWT 令牌
            refresh = RefreshToken.for_user(user)
            response.data = {
                'refresh': str(refresh),
                'access': str(refresh.access_token),
                'email': user.email,
                'is_new_user': is_new_user
            }
            
            logger.info(f"Google login response: {response.data}")
            return response
        except Exception as e:
            logger.error(f"Google login error: {str(e)}", exc_info=True)
            return Response({"error": str(e), "details": traceback.format_exc()}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def register_new_user(self, user, extra_data):
        try:
            # 设置用户的 username 为 email
            user.username = extra_data.get('email', user.email)
            user.save()

            # 授予试用会员资格
            trial_type = MembershipType.objects.get(name="Trial")
            Membership.objects.create(user=user, membership_type=trial_type)

            # 发送欢迎邮件
            subject = 'Welcome to GuizhenIntel'
            message = f'Your account has been successfully created, and you have obtained a GuizhenIntel Trial Plan.'
            from_email = settings.DEFAULT_FROM_EMAIL
            recipient_list = [user.email]
            bcc_list = [settings.BCC_EMAIL]
            send_notification_email.delay(subject, message, from_email, recipient_list, bcc_list)

            logger.info(f"New user registered via Google: {user.email}")
        except Exception as e:
            logger.error(f"Error registering new Google user: {str(e)}", exc_info=True)

class ForgetPasswordView(APIView):
    permission_classes = []  # 允许未认证用户访问
    
    def post(self, request):
        email = request.data.get('email')
        if not email:
            return Response(
                {'error': 'please provide email address'},
                status=status.HTTP_400_BAD_REQUEST
            )
            
        try:
            user = User.objects.get(email=email)
            # 生成重置令牌
            token = default_token_generator.make_token(user)
            uid = urlsafe_base64_encode(force_bytes(user.pk))
            
            FRONTEND_URL = settings.CORS_ALLOWED_ORIGINS[2]
            # 构建重置链接
            reset_link = f"{FRONTEND_URL}/reset-password/{uid}/{token}"
            
            # 使用异步任务发送邮件
            subject = 'GuizhenIntel Password Reset'
            message = f'Please click the following link to reset your password:\n\n{reset_link}'
            from_email = settings.DEFAULT_FROM_EMAIL
            recipient_list = [email]
            bcc_list = [settings.BCC_EMAIL]
            
            send_notification_email.delay(subject, message, from_email, recipient_list, bcc_list)
            
            return Response({'message': 'The password reset link has been sent to your email'})
            
        except User.DoesNotExist:
            # 为了安全，即使用户不存在也返回成功消息
            return Response({'message': 'The password reset link has been sent to your email'})

class ResetPasswordView(APIView):
    permission_classes = []  # 允许未认证用户访问

    def post(self, request):
        # 从请求中获取参数
        uid = request.data.get('uid')
        token = request.data.get('token')
        new_password = request.data.get('password')
        
        try:
            # 解码用户ID并验证token
            uid = force_str(urlsafe_base64_decode(uid))
            user = User.objects.get(pk=uid)
            
            if default_token_generator.check_token(user, token):
                # 设置新密码
                user.set_password(new_password)
                user.save()
                return Response({
                    'message': 'Password has been reset successfully'
                })
            else:
                return Response({
                    'error': 'Invalid or expired reset link'
                }, status=status.HTTP_400_BAD_REQUEST)
                
        except (TypeError, ValueError, OverflowError, User.DoesNotExist):
            return Response({
                'error': 'Invalid reset link'
            }, status=status.HTTP_400_BAD_REQUEST)









