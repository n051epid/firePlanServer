import requests
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from django.http import JsonResponse
from fire_100UpPlan.utils import wechat_token


class WeChatMenuAPIView(APIView):
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        access_token = wechat_token.get_access_token()
        if not access_token:
            return JsonResponse({
                'success': False,
                'error': '获取access_token失败'
            })
            
        # 构建菜单数据
        menu_data = {
            "button": [
                {
                    "type": "click",
                    "name": "数字游民",
                    "key": "digital_nomad",
                    "sub_button": [
                        {
                            "type": "view",
                            "name": "生活方式",
                            "url": "https://planb.qinglv.online/digital-nomad/lifestyle"
                        },
                        {
                            "type": "view",
                            "name": "工作机会",
                            "url": "https://planb.qinglv.online/digital-nomad/jobs"
                        },
                        {
                            "type": "view",
                            "name": "交流讨论",
                            "url": "https://planb.qinglv.online/digital-nomad/community"
                        }
                    ]
                },
                {
                    "name": "资产配置",
                    "type": "click",
                    "key": "asset_allocation",
                    "sub_button": [
                        {
                            "type": "view",
                            "name": "长期投资",
                            "url": "https://planb.qinglv.online/asset/long-term"
                        },
                        {
                            "type": "view",
                            "name": "稳健理财",
                            "url": "https://planb.qinglv.online/asset/stable"
                        },
                        {
                            "type": "view",
                            "name": "保险保障",
                            "url": "https://planb.qinglv.online/asset/insurance"
                        },
                        {
                            "type": "view",
                            "name": "养老计划",
                            "url": "https://planb.qinglv.online/asset/retirement"
                        }
                    ]
                },
                {
                    "name": "FIRE轻旅",
                    "type": "click",
                    "key": "fire_plan",
                    "sub_button": [
                        {
                            "type": "view",
                            "name": "FIRE理念",
                            "url": "https://planb.qinglv.online/fire/concept"
                        },
                        {
                            "type": "view",
                            "name": "躺平自由度",
                            "url": "https://planb.qinglv.online/fire/lay-flat"
                        },
                        {
                            "type": "view",
                            "name": "财务自由度",
                            "url": "https://planb.qinglv.online/fire/financial"
                        }
                    ]
                }
            ]
        }

        # 调用微信接口创建菜单
        create_url = f"https://api.weixin.qq.com/cgi-bin/menu/create?access_token={access_token}"
        response = requests.post(
            create_url, 
            json=menu_data,
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        result = response.json()

        if result.get('errcode') == 0:
            return JsonResponse({
                'success': True,
                'message': '菜单创建成功'
            })
        else:
            return JsonResponse({
                'success': False,
                'error': f"菜单创建失败：{result.get('errmsg')}"
            })
        