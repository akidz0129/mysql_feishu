from app.core.config import DB_CONFIG         # 导入数据库配置
import aiomysql

async def get_connection():
    # 创建数据库连接并返回
    conn = await aiomysql.connect(**DB_CONFIG)
    return conn