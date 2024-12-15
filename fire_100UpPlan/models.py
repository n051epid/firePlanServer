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


# 市场数据基础模型
class BaseMarketData(models.Model):
    """市场数据基础模型"""
    date = models.DateField('交易日期')
    code = models.CharField('代码', max_length=50)
    name = models.CharField('名称', max_length=100)
    created_at = models.DateTimeField('创建时间', auto_now_add=True)

    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=['date', 'code']),
        ]


class MarketValuation(models.Model):
    """市场整体估值数据"""
    date = models.DateField('交易日期')
    # 板块名称
    sector_name = models.CharField('板块名称', max_length=50, default='沪深市场')
    # 静态市盈率和滚动市盈率
    pe_ratio = models.FloatField('静态市盈率', null=True)
    pe_range_percentile = models.FloatField('PE区间百分位', null=True)
    pe_rank_percentile = models.FloatField('PE Rank百分位', null=True)
    pe_ttm_ratio = models.FloatField('滚动市盈率', null=True)
    # 市净率
    pb_ratio = models.FloatField('市净率', null=True)
    pb_range_percentile = models.FloatField('PB区间百分位', null=True)
    pb_rank_percentile = models.FloatField('PB Rank百分位', null=True)
    # 股息率
    dividend_ratio = models.FloatField('股息率', null=True)
    # 股票家数
    stock_count = models.IntegerField('股票家数', null=True)
    # 全市场交易量
    total_volume = models.FloatField('全市场交易量', null=True)
    # 全市场成交额
    total_amount = models.FloatField('全市场成交额', null=True)
    # 全市场温度
    market_temperature = models.FloatField('全市场温度', null=True)
    # 上证指数
    sh_index = models.FloatField('上证指数', null=True)
    sh_pe_rank_percentile = models.FloatField('上证PE Rank百分位', null=True)
    sh_pb_rank_percentile = models.FloatField('上证PB Rank百分位', null=True)

    created_at = models.DateTimeField('创建时间', auto_now_add=True)
    updated_at = models.DateTimeField('更新时间', auto_now=True)

    class Meta:
        db_table = 'market_valuation'
        indexes = [
            models.Index(fields=['date']),
            models.Index(fields=['sector_name']),
        ]
        unique_together = ['date', 'sector_name']
        verbose_name = '市场整体估值'
        verbose_name_plural = verbose_name
        
    def __str__(self):
        return f"{self.sector_name} - {self.date}"

class MarginTradingData(models.Model):
    """两融数据模型"""
    date = models.DateField(verbose_name="日期")
    margin_balance = models.FloatField(verbose_name="融资余额", help_text="单位: 亿")
    securities_balance = models.FloatField(verbose_name="融券余额", help_text="单位: 亿")
    margin_buy = models.FloatField(verbose_name="融资买入额", help_text="单位: 亿")
    securities_sell = models.FloatField(verbose_name="融券卖出额", help_text="单位: 亿")
    securities_firms = models.FloatField(verbose_name="证券公司数量", help_text="单位: 家")
    branches = models.FloatField(verbose_name="营业部数量", help_text="单位: 家")
    individual_investors = models.FloatField(verbose_name="个人投资者数量", help_text="单位: 万名")
    institutional_investors = models.FloatField(verbose_name="机构投资者数量", help_text="单位: 家")
    active_traders = models.FloatField(verbose_name="参与交易的投资者数量", help_text="单位: 名")
    margin_traders = models.FloatField(verbose_name="有融资融券负债的投资者数量", help_text="单位: 名")
    collateral_value = models.FloatField(verbose_name="担保物总价值", help_text="单位: 亿")
    maintenance_ratio = models.FloatField(verbose_name="平均维持担保比例", help_text="单位: %")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    class Meta:
        verbose_name = "两融数据"
        verbose_name_plural = verbose_name
        db_table = "margin_trading_data"
        ordering = ["-date"]
        unique_together = ["date"]  # 日期作为唯一索引
        verbose_name = '两融数据'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"两融数据 - {self.date}"

class IndustryValuation(models.Model):
    """行业估值数据"""
    date = models.DateField('交易日期')
    industry_code = models.CharField('行业代码', max_length=10)
    industry_name = models.CharField('行业名称', max_length=100)
    static_pe = models.FloatField('静态市盈率', null=True)
    ttm_pe = models.FloatField('滚动市盈率', null=True)
    pb_ratio = models.FloatField('市净率', null=True)
    dividend_ratio = models.FloatField('股息率', null=True)
    stock_count = models.IntegerField('股票家数')
    parent_industry = models.CharField('父级行业', max_length=100, null=True)  # 例如"A"类的"农、林、牧、渔业"
    ttm_pe_min = models.FloatField('TTM PE最小值', null=True)
    ttm_pe_mean = models.FloatField('TTM PE均值', null=True)
    ttm_pe_std = models.FloatField('TTM PE标准差', null=True)
    ttm_pe_median = models.FloatField('TTM PE中位数', null=True)
    ttm_pe_percentiles = models.JSONField('TTM PE分位数', null=True)
    ttm_pe_is_normal = models.BooleanField('TTM PE是否符合正态分布', null=True)
    pb_ratio_min = models.FloatField('PB最小值', null=True)
    pb_ratio_mean = models.FloatField('PB均值', null=True)
    pb_ratio_std = models.FloatField('PB标准差', null=True)
    pb_ratio_median = models.FloatField('PB中位数', null=True)
    pb_ratio_percentiles = models.JSONField('PB分位数', null=True)
    pb_ratio_is_normal = models.BooleanField('PB是否符合正态分布', null=True)
    
    created_at = models.DateTimeField('创建时间', auto_now_add=True)
    updated_at = models.DateTimeField('更新时间', auto_now=True)

    class Meta:
        db_table = 'industry_valuation'
        indexes = [
            models.Index(fields=['date']),
            models.Index(fields=['industry_code']),
            models.Index(fields=['parent_industry']),
        ]
        unique_together = ['date', 'industry_code']
        verbose_name = '行业估值'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"{self.industry_name} ({self.industry_code}) - {self.date}"


class StockData(BaseMarketData):
    """股票数据"""
    open = models.FloatField('开盘价')
    high = models.FloatField('最高价')
    low = models.FloatField('最低价')
    close = models.FloatField('收盘价')
    volume = models.BigIntegerField('成交量')
    amount = models.FloatField('成交额')
    turnover = models.FloatField('换手率', null=True)
    pe_ratio = models.FloatField('市盈率', null=True)
    pe_ttm_ratio = models.FloatField('滚动市盈率', null=True)
    pb_ratio = models.FloatField('市净率', null=True)
    dividend_ratio = models.FloatField('股息率', null=True)
    industry_name = models.CharField('所属行业', max_length=100, null=True)
    industry_code = models.CharField('行业代码', max_length=10, null=True)
    parent_industry_code = models.CharField('父级行业代码', max_length=10, null=True)
    parent_industry_pb_ratio_median = models.FloatField('父级行业PB中位数', null=True)
    parent_industry_pe_ttm_ratio_median = models.FloatField('父级行业TTM PE中位数', null=True)
    total_market_value = models.FloatField('总市值', null=True)
    float_market_value = models.FloatField('流通市值', null=True)
    sixty_day_increase = models.FloatField('近60日涨幅', null=True)
    year_to_date_increase = models.FloatField('今年以来涨幅', null=True)

    class Meta:
        db_table = 'market_stock_data'
        unique_together = [] # 移除联合唯一键
        indexes = [
            models.Index(fields=['date', 'code']),
        ]
        verbose_name = '股票数据'
        verbose_name_plural = verbose_name

class BigDataStrategyStockData(BaseMarketData):
    """大数投资策略股票数据"""
    close = models.FloatField('收盘价')
    one_year_min = models.FloatField('近一年最低价', null=True)
    pe_ratio = models.FloatField('市盈率', null=True)
    pe_ttm_ratio = models.FloatField('滚动市盈率', null=True)
    pb_ratio = models.FloatField('市净率', null=True)
    dividend_ratio = models.FloatField('股息率', null=True)
    industry_name = models.CharField('所属行业', max_length=100, null=True)
    industry_code = models.CharField('行业代码', max_length=10, null=True)
    parent_industry_code = models.CharField('父级行业代码', max_length=10, null=True)
    total_market_value = models.FloatField('总市值', null=True)
    sixty_day_increase = models.FloatField('近60日涨幅', null=True)
    year_to_date_increase = models.FloatField('今年以来涨幅', null=True)
    one_year_increase = models.FloatField('近一年涨幅', null=True)
    bigdata_score = models.FloatField('大数得分', null=True)
    bigdata_score_method = models.CharField('打分方式', max_length=50, null=True)

    class Meta:
        db_table = 'market_bigdata_strategy_stock_data'
        unique_together = [] # 移除联合唯一键
        indexes = [
            models.Index(fields=['date', 'code']),
        ]
        verbose_name = '大数股票池'
        verbose_name_plural = verbose_name

class IndexData(BaseMarketData):
    """指数数据,percentile 为rank百分位,即价格代表情绪"""
    open = models.FloatField('开盘点位',null=True)
    high = models.FloatField('最高点位',null=True)
    low = models.FloatField('最低点位',null=True)
    close = models.FloatField('收盘点位',null=True)
    volume = models.BigIntegerField('成交量',null=True)
    amount = models.FloatField('成交额',null=True)
    pe_ratio = models.FloatField('市盈率', null=True)
    pe_ttm_ratio = models.FloatField('滚动市盈率', null=True)
    pb_ratio = models.FloatField('市净率', null=True)
    percentile = models.FloatField('百分位', null=True)

    class Meta:
        db_table = 'market_index_data'
        unique_together = ('date', 'code')
        verbose_name = '指数数据'
        verbose_name_plural = verbose_name


class BondData(BaseMarketData):
    """可转债数据"""
    subscription_date = models.DateField('申购日期', null=True)
    subscription_code = models.CharField('申购代码', max_length=50, null=True)
    subscription_announcement_date = models.DateField('中签号发布日', null=True)
    subscription_record_date = models.DateField('原股东配售-股权登记日', null=True)
    subscription_per_share = models.FloatField('原股东配售-每股配售额', null=True)
    subscription_rate = models.FloatField('中签率', null=True)
    listing_date = models.DateField('上市时间', null=True)
    close = models.FloatField('现价')
    stock_code = models.CharField('正股代码', max_length=50, null=True)
    stock_name = models.CharField('正股名称', max_length=100, null=True)
    stock_price = models.FloatField('正股价', null=True)
    stock_pb = models.FloatField('正股PB', null=True) 
    stock_industry_pb_ratio_median = models.FloatField('行业中位数PB', null=True) 
    stock_pe_ttm_ratio = models.FloatField('正股TTM PE', null=True) 
    stock_industry_pe_ttm_ratio_median = models.FloatField('行业中位数TTM PE', null=True) 
    convertible_price = models.FloatField('转股价', null=True)
    convertible_value = models.FloatField('转股价值', null=True)
    premium_rate = models.FloatField('转股溢价率', null=True)  # 单位: %
    bond_rating = models.CharField('债券评级', max_length=50, null=True)
    issue_size = models.FloatField('发行规模', null=True)  # 单位: 亿元
    remaining_size = models.FloatField('剩余规模', null=True)  # 单位: 亿元
    ytm_before_tax = models.FloatField('到期税前收益', null=True)  # 单位: % # 无
    maturity_date = models.DateField('到期时间', null=True)
    double_low = models.FloatField('双低', null=True) 
    
    # 强赎相关信息
    is_callable = models.CharField('强赎状态', max_length=50, null=True)
    redemption_trigger_price = models.FloatField('强赎触发价', null=True)
    redemption_price = models.FloatField('强赎价格', null=True)
    redemption_countdown = models.CharField('强赎倒计时', max_length=50, null=True)
    convertible_start_date = models.DateField('转股起始日', null=True)
    last_trading_date = models.DateField('最后交易日', null=True)

    class Meta:
        db_table = 'market_bond_data'
        unique_together = []  # 移除联合唯一键
        indexes = [
            models.Index(fields=['code']),  # 为 code 字段创建索引
            models.Index(fields=['date']),  # 为 date 字段创建索引
        ]
        verbose_name = '可转债数据'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"{self.name}({self.code}) - {self.date}"

class BondIndexData(BaseMarketData):
    """可转债指数数据"""
    price = models.FloatField('指数', null=True)
    increase_rt = models.FloatField('涨幅', null=True)
    avg_price = models.FloatField('平均价格(元)', null=True)
    mid_price = models.FloatField('中位数价格(元)', null=True)
    avg_premium_rt = models.FloatField('平均溢价率', null=True)
    mid_premium_rt = models.FloatField('中位数溢价率', null=True)
    avg_ytm_rt = models.FloatField('平均收益率', null=True)
    
    # 价格分布统计
    price_90 = models.IntegerField('>90', null=True)
    price_90_100 = models.IntegerField('90~100', null=True)
    price_100_110 = models.IntegerField('100~110', null=True)
    price_110_120 = models.IntegerField('110~120', null=True)
    price_120_130 = models.IntegerField('120~130', null=True)
    price_130 = models.IntegerField('>130', null=True)

    class Meta:
        db_table = 'market_bond_index_data'
        unique_together = ('date', 'code')
        verbose_name = '可转债指数'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"{self.name}({self.code}) - {self.date}"

class FundData(BaseMarketData):
    """基金数据"""
    nav = models.FloatField('净值')
    acc_nav = models.FloatField('累计净值')
    daily_return = models.FloatField('日收益率')
    type = models.CharField('基金类型', max_length=50)
    
    class Meta:
        db_table = 'market_fund_data'
        unique_together = ('date', 'code')
        verbose_name = '基金数据'
        verbose_name_plural = verbose_name

