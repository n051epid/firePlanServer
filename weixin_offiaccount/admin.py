from django.contrib import admin
from django.contrib import messages
from .models import WeChatMenu
from .utils import wechat_token_qinglv, wechat_token_black13eard
import requests
import json


@admin.register(WeChatMenu)
class WeChatMenuAdmin(admin.ModelAdmin):
    list_display = ['name', 'type', 'parent', 'order', 'updated_at']
    list_filter = ['type', 'parent']
    search_fields = ['name', 'key', 'url']
    ordering = ['order']
    
    actions = ['sync_to_wechat']
    
    def sync_to_wechat(self, request, queryset):
        # 构建菜单数据
        root_menus = WeChatMenu.objects.filter(parent=None).order_by('order')
        menu_data = {"button": []}
        
        for root_menu in root_menus[:3]:  # 最多3个一级菜单
            menu_item = {
                "name": root_menu.name,
                "type": root_menu.type,
            }
            
            if root_menu.type == 'click':
                menu_item["key"] = root_menu.key
            elif root_menu.type == 'view':
                menu_item["url"] = root_menu.url
                
            # 添加子菜单
            sub_buttons = root_menu.sub_buttons.all().order_by('order')
            if sub_buttons.exists():
                menu_item["sub_button"] = []
                for sub_menu in sub_buttons[:5]:  # 最多5个二级菜单
                    sub_item = {
                        "type": sub_menu.type,
                        "name": sub_menu.name
                    }
                    if sub_menu.type == 'click':
                        sub_item["key"] = sub_menu.key
                    elif sub_menu.type == 'view':
                        sub_item["url"] = sub_menu.url
                    menu_item["sub_button"].append(sub_item)
                    
            menu_data["button"].append(menu_item)
            
        print("Update Menu: ", menu_data)

        # 调用微信接口
        access_token = wechat_token_qinglv.get_access_token()
        access_token2 = wechat_token_black13eard.get_access_token()
        print("access_token_qinglv: ", access_token)
        print("access_token_black13eard: ", access_token2)

        if not access_token:
            self.message_user(request, "获取access_token失败", level=messages.ERROR)
            return
            
        create_url = f"https://api.weixin.qq.com/cgi-bin/menu/create?access_token={access_token}"
        response = requests.post(
            create_url,
            data=json.dumps(menu_data, ensure_ascii=False).encode('utf-8'),
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        result = response.json()
        
        if result.get('errcode') == 0:
            self.message_user(request, "菜单同步成功", level=messages.SUCCESS)
        else:
            self.message_user(
                request, 
                f"菜单同步失败：{result.get('errmsg')}", 
                level=messages.ERROR
            )
    
    sync_to_wechat.short_description = "同步菜单到微信"

