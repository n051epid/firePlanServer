from django.contrib import admin
from django.urls import path, reverse
from django.http import HttpResponseRedirect
from django.template.response import TemplateResponse
from django.utils.html import format_html
from django.utils import timezone
from datetime import timedelta
from .models import Membership, MembershipType, MembershipPurchase
from django.contrib.auth.models import Group,User
from django.contrib.auth.admin import GroupAdmin,UserAdmin
from .models import (
    StockData, 
    BigDataStrategyStockData,
    IndexData, 
    FundData, 
    BondData, 
    MarketValuation,
    MembershipType,
    Membership,
    MembershipPurchase,
    MarginTradingData,
    IndustryValuation,
    BondIndexData,
)
from django.utils.translation import gettext_lazy as _
from django.contrib import messages
import logging

logger = logging.getLogger(__name__)

# 会员系统分组
class MembershipAdmin(admin.ModelAdmin):
    """会员管理基类"""
    
    def get_app_list(self, request):
        """
        自定义会员管理分组显示
        """
        app_dict = self._build_app_dict(request)
        app_dict['name'] = _('会员管理')  # 支持国际化
        app_dict['app_label'] = 'membership'  # 设置应用标签
        return [app_dict]
    
@admin.register(Membership)
class MembershipAdmin(MembershipAdmin):
    list_display = ('user', 'membership_type', 'start_date', 'end_date', 'is_active')
    list_filter = ('membership_type', 'is_active')
    search_fields = ('user__username', 'user__email')
    app_label = 'membership'

@admin.register(MembershipType)
class MembershipTypeAdmin(MembershipAdmin):
    list_display = ('name', 'duration_days', 'price', 'is_active', 'original_price', 'description')
    list_filter = ('is_active', 'has_basic_features', 'has_advanced_features')
    search_fields = ('name',)
    app_label = 'membership'

@admin.register(MembershipPurchase)
class MembershipPurchaseAdmin(MembershipAdmin):
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
    app_label = 'membership'

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
    # 保留原有的列并添加格式化的 date_joined
    list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff', 'formatted_date_joined', 'formatted_last_login', 'get_groups')
    
    # 添加日期筛选
    list_filter = ('is_staff', 'is_superuser', 'is_active', 'groups', 'date_joined')
    
    def formatted_date_joined(self, obj):
        if obj.date_joined:
            return obj.date_joined.astimezone(timezone.get_current_timezone()).strftime('%Y-%m-%d %H:%M:%S')
        return '-'
    formatted_date_joined.short_description = 'Date joined'
    formatted_date_joined.admin_order_field = 'date_joined'  # 允许排序

    def formatted_last_login(self, obj):
        if obj.last_login:
            return obj.last_login.astimezone(timezone.get_current_timezone()).strftime('%Y-%m-%d %H:%M:%S')
        return '-'
    formatted_last_login.short_description = 'Last login'
    formatted_last_login.admin_order_field = 'last_login'  # 允许排序
    
    def get_groups(self, obj):
        # 获取用户组并以逗号分隔显示
        return ", ".join([group.name for group in obj.groups.all()])
    
    # 设置列标题
    get_groups.short_description = 'Groups'

# 重新注册 User 模型
admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)




# 市场数据分组
class MarketDataAdmin(admin.ModelAdmin):
    """市场数据管理基类"""
    
    def get_app_list(self, request):
        """
        自定义应用分组显示
        """
        app_dict = self._build_app_dict(request)
        app_dict['name'] = _('市场数据')  # 支持国际化
        app_dict['app_label'] = 'market_data'  # 设置应用标签
        return [app_dict]

@admin.register(MarketValuation)
class MarketValuationAdmin(MarketDataAdmin):
    list_display = ('date', 'sector_name', 'market_temperature', 'pe_ratio', 'pe_range_percentile', 'pe_rank_percentile', 'pe_ttm_ratio', 'pb_ratio', 'pb_range_percentile', 'pb_rank_percentile', 'dividend_ratio', 'stock_count', 'total_volume', 'total_amount', 'sh_index', 'sh_pe_rank_percentile', 'sh_pb_rank_percentile')
    date_hierarchy = 'date'
    ordering = ('-date',)
    app_label = 'market_data'

@admin.register(IndexData)
class IndexDataAdmin(MarketDataAdmin):
    list_display = ('date', 'code', 'name', 'close', 'pe_ratio', 'pe_ttm_ratio', 'pb_ratio', 'percentile')
    list_filter = ('date','name')
    search_fields = ('code', 'name')
    date_hierarchy = 'date'
    app_label = 'market_data'


@admin.register(MarginTradingData)
class MarginTradingDataAdmin(MarketDataAdmin):
    list_display = ('date', 'margin_balance', 'securities_balance', 'margin_buy', 'securities_sell', 'securities_firms', 'branches', 'individual_investors', 'institutional_investors', 'active_traders', 'margin_traders', 'collateral_value', 'maintenance_ratio')
    date_hierarchy = 'date'
    app_label = 'market_data'

@admin.register(StockData)
class StockDataAdmin(MarketDataAdmin):
    list_display = (
        'code', 
        'name', 
        'close', 
        'pe_ratio', 
        'pe_ttm_ratio', 
        'pb_ratio', 
        'industry_name',
        'industry_code',
        'parent_industry_code',
        'parent_industry_pb_ratio_median',
        'parent_industry_pe_ttm_ratio_median',
        'total_market_value',
        'float_market_value',
        'sixty_day_increase',
        'year_to_date_increase',
    )
    list_filter = ('industry_name', 'industry_code', 'parent_industry_code')
    search_fields = ('code', 'name')
    date_hierarchy = 'date'
    app_label = 'market_data'


@admin.register(BigDataStrategyStockData)
class BigDataStrategyStockDataAdmin(MarketDataAdmin):
    list_display = ('code', 'name', 'bigdata_score', 'close', 'one_year_min', 'pe_ratio', 'pe_ttm_ratio', 'pb_ratio', 'dividend_ratio','industry_name', 'industry_code', 'parent_industry_code', 'total_market_value', 'sixty_day_increase', 'year_to_date_increase', 'one_year_increase', 'bigdata_score_method')
    list_filter = ('date', 'industry_name', 'industry_code', 'parent_industry_code', 'bigdata_score_method')
    search_fields = ('code', 'name')
    date_hierarchy = 'date'
    ordering = ('-bigdata_score',)
    app_label = 'market_data'


@admin.register(FundData)
class FundDataAdmin(MarketDataAdmin):
    list_display = ('date', 'code', 'name', 'nav', 'acc_nav', 'daily_return', 'type')
    list_filter = ('date', 'type')
    search_fields = ('code', 'name')
    date_hierarchy = 'date'
    app_label = 'market_data'


@admin.register(BondIndexData)
class BondIndexDataAdmin(MarketDataAdmin):
    list_display = ('date', 'code', 'name', 'price', 'increase_rt', 'avg_price', 'mid_price', 'avg_premium_rt', 'mid_premium_rt', 'avg_ytm_rt', 'price_90', 'price_90_100', 'price_100_110', 'price_110_120', 'price_120_130', 'price_130')
    search_fields = ('code', 'name')
    date_hierarchy = 'date'
    app_label = 'market_data'

@admin.register(BondData)
class BondDataAdmin(MarketDataAdmin):
    list_display = ('code', 'name', 'close', 'double_low', 'redemption_countdown', 'is_callable', 'redemption_trigger_price', 'redemption_price', 'convertible_start_date', 'last_trading_date', 'stock_code', 'stock_name', 'stock_price', 'stock_pb', 'stock_industry_pb_ratio_median', 'stock_pe_ttm_ratio', 'stock_industry_pe_ttm_ratio_median', 'convertible_price', 'convertible_value', 'premium_rate', 'bond_rating', 'remaining_size', 'ytm_before_tax', 'maturity_date', 'listing_date', 'is_risk_excluded')
    list_filter = ('is_callable', 'bond_rating', 'is_risk_excluded')
    search_fields = ('code', 'name')
    date_hierarchy = 'date'
    ordering = ('premium_rate','remaining_size','double_low')
    app_label = 'market_data'


@admin.register(IndustryValuation)
class IndustryValuationAdmin(admin.ModelAdmin):
    list_display = [
        'date',
        'industry_code',
        'industry_name',
        'parent_industry',
        'static_pe',
        'ttm_pe',
        'ttm_pe_min',
        'pb_ratio',
        'pb_ratio_min',
        'dividend_ratio',
        'stock_count',
        'ttm_pe_mean',
        'ttm_pe_std',
        'ttm_pe_median',
        'ttm_pe_percentiles',
        'ttm_pe_is_normal',
        'pb_ratio_mean',
        'pb_ratio_std',
        'pb_ratio_median',
        'pb_ratio_percentiles',
        'pb_ratio_is_normal',
    ]
    
    list_filter = [
        'date',
        'parent_industry',
        'industry_code',
    ]

    date_hierarchy = 'date'

    search_fields = [
        'industry_code',
        'industry_name',
        'parent_industry',
    ]
    
    ordering = ['-date', 'industry_code']
    
    readonly_fields = [
        'created_at',
        'updated_at',
    ]
    
    fieldsets = [
        ('基本信息', {
            'fields': (
                'date',
                'industry_code',
                'industry_name',
                'parent_industry',
            )
        }),
        ('估值指标', {
            'fields': (
                'static_pe',
                'ttm_pe',
                'pb_ratio',
                'dividend_ratio',
            )
        }),
        ('统计数据', {
            'fields': (
                'stock_count',
            )
        }),
        ('系统信息', {
            'fields': (
                'created_at',
                'updated_at',
            ),
            'classes': ('collapse',)  # 默认折叠
        }),
    ]
    
    def has_add_permission(self, request):
        # 禁止手动添加数据
        return False
    
    def has_delete_permission(self, request, obj=None):
        # 禁止删除数据
        return False
