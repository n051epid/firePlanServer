from django.contrib import admin
from django.urls import path, reverse
from django.http import HttpResponseRedirect
from django.template.response import TemplateResponse
from django.utils.html import format_html
from django.utils import timezone
from datetime import timedelta
from .models import Membership, MembershipType, MembershipPurchase
from django.contrib.auth.models import Group
from django.contrib.auth.admin import GroupAdmin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User

# Register your models here.

@admin.register(Membership)
class MembershipAdmin(admin.ModelAdmin):
    list_display = ('user', 'membership_type', 'start_date', 'end_date', 'is_active')
    list_filter = ('membership_type', 'is_active')
    search_fields = ('user__username', 'user__email')

@admin.register(MembershipType)
class MembershipTypeAdmin(admin.ModelAdmin):
    list_display = ('name', 'duration_days', 'price', 'is_active', 'original_price', 'description')
    list_filter = ('is_active', 'has_basic_features', 'has_advanced_features')
    search_fields = ('name',)

# 移除重复注册的代码
# admin.site.register(MembershipType, MembershipTypeAdmin)

@admin.register(MembershipPurchase)
class MembershipPurchaseAdmin(admin.ModelAdmin):
    list_display = ['user', 'membership_type', 'payment_status', 'purchase_date', 'amount_paid', 'refund_button']
    list_filter = ['payment_status', 'purchase_date']
    search_fields = ['user__username', 'transaction_id']
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('<path:object_id>/mark-refund/', self.admin_site.admin_view(self.mark_refund), name='membershippurchase-mark-refund'),
        ]
        return custom_urls + urls
    
    def mark_refund(self, request, object_id, *args, **kwargs):
        purchase = self.get_object(request, object_id)
        
        # 获取与购买相关的会员信息
        membership = Membership.objects.filter(purchase=purchase).first()

        if membership:
            order_start_date = membership.start_date
            order_end_date = membership.end_date
        else:
            # 如果找不到对应的会员记录，使用购买日期作为开始日期
            order_start_date = purchase.purchase_date
            order_end_date = order_start_date + timedelta(days=purchase.membership_type.duration_days)

        if request.method != 'POST':
            # 获取当前正在生效的会员信息
            current_active_membership = Membership.objects.filter(
                user=purchase.user,
                is_active=True,
                end_date__gt=timezone.now()
            ).order_by('-end_date').first()

            if membership:
                membership_duration = membership.membership_type.duration_days
            else:
                # 如果找不到对应的会员记录，使用购买日期作为开始日期
                membership_duration = purchase.membership_type.duration_days

            current_end_date = current_active_membership.end_date if current_active_membership else None

            # 初始化 new_end_date
            new_end_date = timezone.now()

            # 判断订单是否已经生效
            if order_end_date > timezone.now():
                # 订单还未生效
                refund_amount = purchase.amount_paid
                action_description = "此订单对应的会员记录将被删除。"
            else:
                # 订单正在生效
                total_duration = membership_duration
                used_duration = timezone.now() - order_start_date
                refund_ratio = max((total_duration - used_duration) / total_duration, 0)
                refund_amount = purchase.amount_paid * refund_ratio
                action_description = "会员资格将被调整到当前日期。"

            context = {
                'purchase': purchase,
                'current_membership_end_date': current_end_date,
                'new_membership_end_date': new_end_date,
                'refund_amount': round(refund_amount, 2),
                'action_description': action_description,
            }
            return TemplateResponse(request, 'admin/mark_refund_confirmation.html', context)
        
        else:
            try:
                # 如果退款订单还未生效，则删除会员记录
                if order_end_date > timezone.now():
                    purchase.payment_status = 'REFUNDED'
                    purchase.refund_date = timezone.now()
                    purchase.save()
                else:
                    # 如果退款订单正在生效，则调整当前会员资格的结束日期
                    current_membership = Membership.objects.filter(user=purchase.user, is_active=True).first()
                    if current_membership:
                        current_membership.end_date = timezone.now()
                        current_membership.save()

                self.message_user(request, '退款状态已更新为待处理，相应的会员资格已被调整')
            except Exception as e:
                self.message_user(request, f'更新退款状态时发生错误: {str(e)}', level='ERROR')
            return HttpResponseRedirect("../")
    
    def refund_button(self, obj):
        if obj.payment_status == 'COMPLETED':
            url = reverse('admin:membershippurchase-mark-refund', args=[obj.pk])
            return format_html('<a class="button" href="{}">标记为退款待处理</a>', url)
        return ''
    refund_button.short_description = '操作'
    refund_button.allow_tags = True

    readonly_fields = ('refund_button',)

# 用户组增加「用户数量」列
class CustomGroupAdmin(GroupAdmin):
    list_display = ('name', 'get_users_count', 'view_users')
    
    def get_users_count(self, obj):
        return obj.user_set.count()
    get_users_count.short_description = 'number of users'

    def view_users(self, obj):
        # 创建一个链接到用户列表的按钮,过滤该组的用户
        url = f'/admin/auth/user/?groups__id__exact={obj.id}'
        return format_html('<a href="{}">view members</a>', url)
    view_users.short_description = 'view members'

# 重新注册 Group
admin.site.unregister(Group)
admin.site.register(Group, CustomGroupAdmin)

class CustomUserAdmin(UserAdmin):
    # 保留原有的列
    list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff', 'get_groups')
    
    def get_groups(self, obj):
        # 获取用户组并以逗号分隔显示
        return ", ".join([group.name for group in obj.groups.all()])
    
    # 设置列标题
    get_groups.short_description = 'Groups'

# 重新注册 User 模型
admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)
