import os
# 数据库连接配置
DB_CONFIG = {
    'host': os.environ.get('MYSQL_HOST'),
    'user': os.environ.get('MYSQL_USER'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'db': os.environ.get('MYSQL_DATABASE'),
    'port': int(os.environ.get('MYSQL_PORT')),
    'charset':'utf8mb4'
}



