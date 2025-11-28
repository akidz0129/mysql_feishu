
# ==== 文件: app/main.py ====
from fastapi import FastAPI , Request              # 引入 FastAPI 框架主类
from app.routers import orders              # 导入订单路由模块
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from app.core.db import get_connection         # 导入数据库连接函数
from app.models import gemini           # 导入路由模块
from app.routers import cal           # 导入路由模块
from app.services import _minio           # 导入路由模块
from app.services import cloudflare           # 导入路由模块
import aiomysql
import uvicorn


app = FastAPI()                           # 创建 FastAPI 实例
templates = Jinja2Templates(directory="templates")

# 注册路由到主应用中
app.include_router(orders.router)    
app.include_router(gemini.router)    
app.include_router(cal.router)       
app.include_router(_minio.router)    
app.include_router(cloudflare.router)

@app.get('/')
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "name": "na"})

@app.post('/search',response_class=HTMLResponse)
async def search(request: Request):
    form = await request.form()
    name = form.get("name")
    conn = None # 初始化连接变量，以便在 finally 块中正确关闭
    table_name="total_orders"
    sql_query = f"SELECT * FROM {table_name} WHERE Product_ID = %s"

    try:
        conn = await get_connection()
        async with conn.cursor(aiomysql.DictCursor) as cursor:
        # 查询  表所有数据
            await cursor.execute(sql_query,(name,))
            result = await cursor.fetchall()            # 获取所有结果

        return templates.TemplateResponse("index.html", {"request": request, "name": name,"data": result})
    finally:
        if conn:
            conn.close()

@app.get('/docs_a')
def index(request: Request):
    return templates.TemplateResponse("docs.html", {"request": request})


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

