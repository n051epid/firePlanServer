from rest_framework import serializers
from django.contrib.auth.models import User
from .models import Membership, MembershipType, MembershipPurchase
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
import logging
from django.contrib.auth.models import Group
from django.conf import settings
from django.utils.translation import gettext_lazy as _

logger = logging.getLogger(__name__)

class UserRegistrationSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)

    class Meta:
        model = User
        fields = ('id', 'username', 'email', 'password')
        extra_kwargs = {'password': {'write_only': True}}

    def create(self, validated_data):
        user = User.objects.create_user(**validated_data)
        # 添加到默认组
        default_group = Group.objects.get(name=settings.DEFAULT_USER_GROUPS['NORMAL_USER'])
        user.groups.add(default_group)
        return user

class MembershipSerializer(serializers.ModelSerializer):
    user = UserRegistrationSerializer(read_only=True)

    class Meta:
        model = Membership
        fields = ['id', 'user', 'membership_type', 'expiry_date']

class MembershipTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = MembershipType
        fields = ['id', 'name', 'duration_days', 'has_basic_features', 'has_advanced_features', 'price', 'is_active', 'original_price', 'description']

class MembershipPurchaseSerializer(serializers.ModelSerializer):
    class Meta:
        model = MembershipPurchase
        fields = ['id', 'membership_type', 'payment_status', 'transaction_id', 'purchase_date', 'amount_paid']

class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    default_error_messages = {
        'no_active_account': _('Username or Password Error')  # 自定义错误消息
    }

    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        
        # 可以在这里添加自定义声明
        # token['username'] = user.username
        # 如果需要，可以添加更多用户信息
        # token['email'] = user.email
        
        return token

    def validate(self, attrs):
        try:
            data = super().validate(attrs)
            # 打印用户所有字段，用于调试
            # logger.info(f"User fields: {user.__dict__}")

            # 构建新的响应格式
            response = {
                "code": 0,
                "data": {
                    "accessToken": data.pop('access'),
                    "refreshToken": data.pop('refresh'),
                },
                "error": None,
                "message": "Login Successful"
            }
            return response
        except Exception as e:
            return {
                "code": 1,
                "data": None,
                "error": str(e),
                "message": "Username or Password Error"
            }




