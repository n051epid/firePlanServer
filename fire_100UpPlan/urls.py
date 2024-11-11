from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenRefreshView
from .views import (
    RegisterView, UserInfoView, PayPalWebhookView, CreatePayPalOrderView,
    CapturePayPalOrderView, MembershipTypeListView, CheckPaymentStatusView,
    PurchaseHistoryView, PaymentSuccessView, PaymentCancelView,
    ActivateAccountView, GoogleLogin, get_csrf_token,
    CustomTokenObtainPairView, UserAuthCodesView, UserLogoutView,
    ForgetPasswordView, ResetPasswordView
)

router = DefaultRouter()
# 如果有任何 ViewSet，在这里注册，例如：
# router.register(r'some-model', SomeModelViewSet)

urlpatterns = [
    path('auth/register/', RegisterView.as_view(), name='register'),
    path('auth/login/', CustomTokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('auth/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('auth/forget-password/', ForgetPasswordView.as_view(), name='forget_password'),
    path('auth/reset-password/', ResetPasswordView.as_view(), name='reset_password'),
    path('user/info/', UserInfoView.as_view(), name='user_info'),
    path('auth/codes/', UserAuthCodesView.as_view(), name='user_auth_codes'),
    path('auth/logout/', UserLogoutView.as_view(), name='user_logout'),
    path('membership-types/', MembershipTypeListView.as_view(), name='membership_types'),
    path('paypal-webhook/', PayPalWebhookView.as_view(), name='paypal_webhook'),
    path('create-paypal-order/', CreatePayPalOrderView.as_view(), name='create_paypal_order'),
    path('capture-paypal-order/', CapturePayPalOrderView.as_view(), name='capture_paypal_order'),
    path('check-payment-status/<str:payment_id>/', CheckPaymentStatusView.as_view(), name='check_payment_status'),
    path('purchase-history/', PurchaseHistoryView.as_view(), name='purchase_history'),
    path('payment/success/', PaymentSuccessView.as_view(), name='payment_success'),
    path('payment/cancel/', PaymentCancelView.as_view(), name='payment_cancel'),
    path('activate/<uidb64>/<token>/', ActivateAccountView.as_view(), name='activate_account'),
    path('', include(router.urls)),
    path('accounts/', include('allauth.urls')),
    path('auth/google/', GoogleLogin.as_view(), name='google_login'),
    path('csrf/', get_csrf_token, name='get_csrf_token'),
]
