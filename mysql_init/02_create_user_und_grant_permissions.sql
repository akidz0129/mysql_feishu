-- 创建应用程序用户
-- 'your_app_user'@'%' 允许从任何主机连接，方便 Docker 内部网络通信
-- 在生产环境中，应尽可能限制主机，例如指定服务名或IP
CREATE USER IF NOT EXISTS 'test_user'@'%' IDENTIFIED BY 'Jiaoji123!';

-- 授予该用户对指定数据库和表的最小权限
GRANT SELECT ON test.* TO 'test_user'@'%';

CREATE USER IF NOT EXISTS 'iceberg_user'@'%' IDENTIFIED BY 'iceberg_password';

GRANT ALL PRIVILEGES ON iceberg_catalog.* TO 'iceberg_user'@'%';

-- 6. 刷新权限（在 Docker 启动脚本中可能不是严格必要，但养成习惯是好的）
FLUSH PRIVILEGES;