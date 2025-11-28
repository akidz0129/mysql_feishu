from fastapi import APIRouter             # 导入路由模块
from app.core.db import get_connection         # 导入数据库连接函数
import aiomysql
from fastapi.responses import Response
from app.utils.utils import generate_csv_content

router = APIRouter(prefix="/orders", tags=["Orders"])  # 创建路由，所有路由路径自动加 /orders 前缀，方便统一管理接口路。给这些路由统一打上 Orders 标签，方便 API 文档分组。

@router.get("/orders")
async def read_orders(page_size: int = 100, offset: int = 0):
    conn = None # 初始化连接变量，以便在 finally 块中正确关闭
    try:
        # 获取数据库连接
        conn = await get_connection()
        # 查询  表所有数据
        # 2. 显式创建游标，并指定为字典游标 (aiomysql.DictCursor)
        #    使用 async with 确保游标在使用完毕后自动关闭

        async with conn.cursor(aiomysql.DictCursor) as cursor:
        # 3. 异步执行 SQL 查询
            results = []

            # await cursor.execute("SELECT * FROM total_orders")
            
            # 4. 异步获取所有结果，由于使用了 DictCursor，结果将是字典的列表
            # results = await cursor.fetchall()
            await cursor.execute("SELECT * FROM total_orders LIMIT %s OFFSET %s", (page_size, offset))
            results = await cursor.fetchall()
            # batch = await cursor.fetchall()
            # results.extend(batch)
        
        # 5. 返回查询结果
        return {"data": results}

    finally:
            # 确保在任何情况下都关闭数据库连接
        if conn:
                # 如果是直接创建的连接，使用 close()。如果使用了连接池，通常是 await conn.release()
                # aiomysql 的 close() 是同步方法，但在异步函数中通常可以接受，因为它的阻塞时间极短。
                # 最佳实践仍是使用连接池。
            conn.close()

from fastapi import APIRouter, Query # 确保导入 Query
@router.get("/single")
async def read_orders(name: str = Query(..., description="要查询的 ASIN 编码")):
    conn = None # 初始化连接变量，以便在 finally 块中正确关闭
    try:

        # 获取数据库连接
        conn = await get_connection()
        # 查询
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT COUNT(*) FROM total_orders where Tags = %s",(name,))
            order_count = await cursor.fetchone()
            await cursor.execute("SELECT SUM(price * sale) FROM total_orders where Tags=%s",(name,))
            total_sale = await cursor.fetchone()


        return {"data":{"order_count":order_count[0],"total_sale":total_sale[0]}}               # 返回查询结果

    finally:
            # 确保在任何情况下都关闭数据库连接
        if conn:
            conn.close()

MOCK_ORDERS = [
    {'order_id': 'ORD001', 'customer_name': '张三', 'product_name': '笔记本电脑', 'total_amount': 7999.00, 'order_date': '2025-07-01'},
    {'order_id': 'ORD002', 'customer_name': '李四', 'product_name': '机械键盘', 'total_amount': 599.00, 'order_date': '2025-07-02'},
    {'order_id': 'ORD003', 'customer_name': '王五', 'product_name': '鼠标', 'total_amount': 199.00, 'order_date': '2025-07-03'},
]
ORDER_FIELDS = ['order_id', 'customer_name', 'product_name', 'total_amount', 'order_date']

@router.get('/export_orders_csv')
async def export_orders_csv():
    conn = None # 初始化连接变量，以便在 finally 块中正确关闭
    try:

        # # 获取数据库连接
        # conn = await get_connection()
        # # 查询
        # async with conn.cursor() as cursor:
        #     await cursor.execute("SELECT COUNT(*) FROM test_t1 where asin = %s",(name,))
        # 1. 从数据库获取数据 (这里用模拟数据代替)
        orders_data = MOCK_ORDERS # 实际应用中会调用 get_orders_for_export()

        # 2. 生成 CSV 内容
        csv_content = generate_csv_content(orders_data, ORDER_FIELDS)

        # 3. 作为 HTTP 响应返回
        response = Response(csv_content, media_type='text/csv; charset=utf-8') 
        response.headers['Content-Disposition'] = 'attachment; filename="orders.csv"'
        return response
    finally:
            # 确保在任何情况下都关闭数据库连接
        if conn:
            conn.close()

import logging
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from typing import List, Optional
from fastapi import FastAPI, HTTPException
# --- 日志设置 (FastAPI 应用内部) ---
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-8s %(levelname)-8s %(message)s [%(filename)s:%(lineno)d]')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --- 请求体模型 ---
class DailyReportRequest(BaseModel):
    # report_date: str = Field(..., example="2023-01-01")
    start_date: str = Field(..., example="2023-01-01")
    end_date: str = Field(..., example="2023-01-01")
    fulfillment_channel: str = Field(None, example="jd_self") 
    msku: Optional[str] = Field(None, example="69xxxx……") 
    columns: List[str] = Field(..., example=["Order_ID", "Quantity", "Total_Amount"])
    remark: str = ""


@router.post('/report')
async def get_daily_report(request: DailyReportRequest):
    logger.info('ss')
    logger.info(
        f"收到日报请求:  "
        f"渠道={request.fulfillment_channel}, 列=, "
    )
    columns_sql = "*" if not request.columns else ", ".join(request.columns)
    # 2. WHERE 子句与参数容器

    conditions = []
    params = []
    if request.start_date and request.end_date:
        conditions.append("DATE(Purchase_Datetime) BETWEEN %s AND %s")
        params.extend([request.start_date, request.end_date])
    elif request.start_date:
        conditions.append("DATE(Purchase_Datetime) >= %s")
        params.append(request.start_date)
    elif request.end_date:
        conditions.append("DATE(Purchase_Datetime) <= %s")
        params.append(request.end_date)
    else:
        return {"status": "error", "message": "必须至少提供一个日期"}
    


    # 3. fulfillment_channel 可选
    if request.fulfillment_channel:
        conditions.append("Fulfillment_Channel = %s")
        params.append(request.fulfillment_channel)

    # 4. MSKU 可选
    if request.msku:
        conditions.append("MSKU = %s")
        params.append(request.msku)
    where_sql = " AND ".join(conditions)
    sql_query = f"SELECT {columns_sql} FROM total_orders WHERE {where_sql}"

    logger.info(sql_query)
    logger.info(f"参数: {params}")

    # --- 3. 执行 SQL 查询 ---
    try:
        conn = await get_connection()
        async with conn.cursor() as cursor:
            logger.info(f"执行SQL: {sql_query} WITH {tuple(params)}")
            await cursor.execute(sql_query, tuple(params))
            result = await cursor.fetchall() # 获取所有结果
            columns = [desc[0] for desc in cursor.description]
        data = [
            dict(zip(columns, row))
            for row in result
        ]

        conn.close()
        
        logger.info(f"查询成功，返回 {len(result)} 条记录。")
        return {
            "status": "success",
            "data": data,
            "total": len(data),
            "message": "日报查询成功。"
        }


    except Exception as e:
        logger.exception(f"处理日报请求时发生未知错误: {e}")
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {e}")

# --- 请求体模型 ---
class DailyItemRequest(BaseModel):
    # report_date: str = Field(..., example="2023-01-01")
    start_date: str = Field(..., example="2023-01-01")
    end_date: str = Field(..., example="2023-01-01")
    fulfillment_channel: str = Field(None, example="jd_self") 
    msku: str = Field(None, example="69xxxx……") 
    columns: List[str] = Field(default_factory=list, example=["Order_ID", "Quantity", "Total_Amount"])
    remark: str = ""

@router.post('/item_info')
async def get_daily_item(request: DailyItemRequest):
    logger.info(
        f"收到请求: 日期=, "
        f"渠道={request.fulfillment_channel}, msku={request.msku}, 列={request.columns}, "
    )
     # --- 1. 处理列名 ---
    if not request.columns:
        return {
            "status": "success",
            "data": [],
            "message": "没有指定列"
        }
    columns_sql = ", ".join(f"SUM({col}) AS {col}" for col in request.columns)

    conditions = []
    params = []

    if request.start_date and request.end_date:
        conditions.append("DATE(Purchase_Datetime) BETWEEN %s AND %s")
        params.extend([request.start_date, request.end_date])
    elif request.start_date:
        conditions.append("DATE(Purchase_Datetime) >= %s")
        params.append(request.start_date)
    elif request.end_date:
        conditions.append("DATE(Purchase_Datetime) <= %s")
        params.append(request.end_date)
    else:
        return {"status": "error", "message": "必须至少提供一个日期"}
    
    # fulfillment_channel
    if request.fulfillment_channel:
        conditions.append("Fulfillment_Channel = %s")
        params.append(request.fulfillment_channel)

    # msku
    if request.msku:
        conditions.append("MSKU = %s")
        params.append(request.msku)

    where_sql = " AND ".join(conditions)

    sql_query = f"""
        SELECT {columns_sql}
        FROM total_orders
        WHERE {where_sql}
    """

    logger.info(sql_query)
    
    logger.info(f"参数: {params}")
    # --- 3. 执行 SQL 查询 ---
    try:
        conn = await get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(sql_query, tuple(params))
            result = await cursor.fetchone()
            logger.info(f"查询结果: {result}")


        conn.close()
        data = dict(zip(request.columns, result)) if result else {}
        
        logger.info(f"查询成功。")
        return {
            "status": "success",
            'msku':request.msku,
            "data": data,
            "message": "查询成功。"
        }


    except Exception as e:
        logger.exception(f"处理日报请求时发生未知错误: {e}")
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {e}")    
    # --- 新增文件下载路由 ---
    
# @router.get("/reports/download/{file_name}")
# async def download_report(file_name: str):
#     file_path = os.path.join(REPORTS_DIR, file_name)

#     if not os.path.exists(file_path):
#         raise HTTPException(status_code=404, detail="文件未找到。")

#     # 返回文件，设定媒体类型以便浏览器正确处理
#     return FileResponse(path=file_path, filename=file_name, media_type='text/csv')
