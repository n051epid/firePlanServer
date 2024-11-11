from django.core.management.base import BaseCommand
from django.contrib.auth.models import Group, Permission

class Command(BaseCommand):
    help = '创建默认用户组'

    def handle(self, *args, **options):
        # 创建基本用户组
        groups = ['Admin', 'Basic', 'Premium']
        
        for group_name in groups:
            group, created = Group.objects.get_or_create(name=group_name)
            if created:
                self.stdout.write(f'Created group: {group_name}') 