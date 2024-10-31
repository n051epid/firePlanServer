#创建虚拟环境
python3 -m venv server_env

# 激活环境
source server_env/bin/activate

# 安装依赖
pip install -r requirements.txt

# 创建项目
django-admin startproject myproject

# git clone 代码，修改项目相关信息及.env 中的配置
	- settings.py 的 ALLOWED_HOSTS_LIST
	- 其他项目名称相关的配置

# 生成迁移文件
python manage.py makemigrations

# 应用迁移
python manage.py migrate

# 创建管理后台用户
python manage.py createsuperuser

# 配置 Nginx 转发服务
使用 myprojectServer.sock 方式

# 创建系统管理服务
sudo vi /etc/systemd/system/myproject.service

ExecStart=/path/server_env/bin/gunicorn --access-logfile - --workers 3 --bind unix:/path/to/server/dir/myprojectServer.sock myprojectServer.wsgi:application

# 运行服务器
sudo systemctl restart parrotranslate
sudo systemctl restart celery_worker
sudo systemctl status redis6
sudo systemctl status nginx

# 访问管理后台地址
https://domain/admin

# 访问服务接口地址
https://domain/v1


