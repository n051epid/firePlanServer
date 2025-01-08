from django.db import models

class WeChatMenu(models.Model):
    MENU_TYPES = [
        ('view', '网页类型'),
        ('click', '点击类型'),
        ('miniprogram', '小程序类型'),
        ('scancode_push', '扫码访问'),
        ('scancode_waitmsg', '扫码带提示'),
        ('pic_sysphoto', '系统拍照发图'),
        ('pic_photo_or_album', '拍照或相册发图'),
        ('pic_weixin', '微信相册发图'),
        ('location_select', '地理位置选择'),
        ('article_id', '图文id'),
        ('article_view_limited', '图文消息'),
    ]

    name = models.CharField('菜单名称', max_length=100)
    type = models.CharField('菜单类型', max_length=20, choices=MENU_TYPES)
    key = models.CharField('菜单key', max_length=100, blank=True, null=True)
    url = models.URLField('跳转链接', blank=True, null=True)
    media_id = models.CharField('素材ID', max_length=100, blank=True, null=True, 
                              help_text='调用新增永久素材接口返回的合法media_id')
    appid = models.CharField('小程序APPID', max_length=100, blank=True, null=True,
                           help_text='小程序的appid（仅认证公众号可配置）')
    pagepath = models.CharField('小程序页面路径', max_length=200, blank=True, null=True,
                              help_text='小程序的页面路径')
    article_id = models.CharField('文章ID', max_length=100, blank=True, null=True,
                                help_text='发布后获得的合法 article_id')
    
    parent = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True, 
                              related_name='sub_buttons', verbose_name='父级菜单')
    order = models.IntegerField('排序', default=0)
    updated_at = models.DateTimeField('更新时间', auto_now=True)

    class Meta:
        verbose_name = '微信菜单'
        verbose_name_plural = '微信菜单'
        ordering = ['order']

    def __str__(self):
        return self.name
    
