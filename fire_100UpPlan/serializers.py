from rest_framework import serializers
from django.contrib.auth.models import User
from .models import Membership, MembershipType, MembershipPurchase
  
class UserSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True)

    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'password']

    def create(self, validated_data):
        user = User.objects.create_user(**validated_data)
        return user

class MembershipSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True)

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

