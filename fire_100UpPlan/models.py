from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
import datetime
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

class MembershipType(models.Model):
    # 套餐的类型
    name = models.CharField(max_length=50)
    duration_days = models.IntegerField()
    has_basic_features = models.BooleanField(default=True)
    has_advanced_features = models.BooleanField(default=False)
    price = models.DecimalField(max_digits=6, decimal_places=2)
    is_active = models.BooleanField(default=True)  # 是否有效
    original_price = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)  # 原价
    description = models.TextField(null=True, blank=True)  # 描述（文本格式）

    def __str__(self):
        return self.name

class Membership(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    membership_type = models.ForeignKey(MembershipType, on_delete=models.PROTECT)
    start_date = models.DateTimeField(default=timezone.now)
    end_date = models.DateTimeField(null=True, blank=True)  # 允许为空
    is_active = models.BooleanField(default=True)
    purchase = models.ForeignKey('MembershipPurchase', on_delete=models.SET_NULL, null=True, blank=True)

    def save(self, *args, **kwargs):
        if not self.end_date:
            self.end_date = self.start_date + datetime.timedelta(days=self.membership_type.duration_days)
        super().save(*args, **kwargs)

    def is_valid(self):
        return self.is_active and self.end_date > timezone.now()

    def __str__(self):
        return f"{self.user.username} - {self.membership_type.name}"

class MembershipPurchase(models.Model):
    PAYMENT_STATUS_CHOICES = [
        ('PENDING', '待处理'),
        ('COMPLETED', '已完成'),
        ('REFUNDED', '已退款'),
        ('REFUND_PENDING', '退款待处理'),
        ('FAILED', '失败'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    membership_type = models.ForeignKey(MembershipType, on_delete=models.CASCADE)
    payment_status = models.CharField(max_length=20, choices=PAYMENT_STATUS_CHOICES, default='PENDING')
    transaction_id = models.CharField(max_length=100, blank=True, null=True)
    purchase_date = models.DateTimeField(auto_now_add=True)
    amount_paid = models.DecimalField(max_digits=10, decimal_places=2)
    refund_date = models.DateTimeField(null=True, blank=True)

    def complete_purchase(self, transaction_id=None, amount_paid=None):
        self.payment_status = 'COMPLETED'
        if transaction_id:
            self.transaction_id = transaction_id
        if amount_paid is not None:
            self.amount_paid = amount_paid
        else:
            self.amount_paid = self.membership_type.price  # 使用会员类型的价格作为默认值
        self.save()
        return update_membership(self.user, self.membership_type)

    def mark_as_refund_pending(self):
        if self.payment_status != 'COMPLETED':
            raise ValueError("只有已完成的支付才能申请退款")
        
        self.payment_status = 'REFUND_PENDING'
        self.refund_date = timezone.now()
        self.save()

        # 记录退款操作
        logger.info(f"会员购买标记为退款待处理: 用户 {self.user.username}, 购买ID {self.id}")

    def adjust_membership(self):
        # 这个方法现在只记录日志，实际的调整逻辑在 admin.py 中处理
        logger.info(f"尝试调整会员资格: 用户 {self.user.username}, 购买ID {self.id}")

    def save(self, *args, **kwargs):
        if self.amount_paid is None:
            self.amount_paid = self.membership_type.price
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.user.username} - {self.membership_type.name} - {self.purchase_date.strftime('%Y-%m-%d %H:%M:%S')}"


