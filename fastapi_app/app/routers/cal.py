from fastapi import APIRouter             # 导入路由模块
from app.core.db import get_connection         # 导入数据库连接函数

import time


router = APIRouter(prefix="/cal", tags=["Cal"])  # 创建路由，所有路由路径自动加 /orders 前缀，方便统一管理接口路。给这些路由统一打上 Orders 标签，方便 API 文档分组。

@router.get("/")
def read_orders():
    start = time.perf_counter()

    # 获取数据库连接
    conn = get_connection()
    cursor = conn.cursor()
    # 查询  表所有数据
    cursor.execute("SELECT COUNT(*) FROM test_t1 ")
    orders = cursor.fetchone()
    # 字段名列表（与表结构一致）
    asin1="B0D1FRQX8Q"
    asin2="B0CZL89YVB"
    cursor.execute("SELECT SUM(price * sale) FROM test_t1 where asin=%s",(asin1,))
    result1 = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM test_t1 where asin=%s",(asin1,))
    orders1 = cursor.fetchone()
    cursor.execute("SELECT SUM(price * sale) FROM test_t1 where asin=%s",(asin2,))
    result2 = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM test_t1 where asin=%s",(asin1,))
    orders2 = cursor.fetchone()
    r1=f"totoal_orders equals {orders[0]}"
    r2=f"A(asin:{asin1}) orders equals {orders1[0]}"
    r3=f"B(asin:{asin2}) orders equals {orders2[0]}"
    r4=f"A sales equals {result1[0]}"
    r5=f"B sales equals {result2[0]}"
    r6=f"( A_sales + B_sales ) / ( A_orders + B_orders ) * total_orders - ( A_sales + B_sales ) = {(result1[0]+result2[0])/(orders1[0]+orders2[0])*orders[0]-(result1[0]+result2[0])}"



    cursor.close()
    conn.close()
    end = time.perf_counter()
    r7=f"运行时间：{end - start:.4f} 秒"
    # print(f"运行时间：{end - start:.4f} 秒")
    return {"data": [r1,r2,r3,r4,r5,r6,r7]}               # 返回查询结果