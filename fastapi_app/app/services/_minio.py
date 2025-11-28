from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi import APIRouter, Request, HTTPException
import os
from minio import Minio
from minio.error import S3Error
import os


router = APIRouter(prefix="/minio", tags=["Minio"])  # 创建路由，所有路由路径自动加 /gemini 前缀，方便统一管理接口路。给这些路由统一打上 Orders 标签，方便 API 文档分组。

templates = Jinja2Templates(directory="templates")


MINIO_ENDPOINT=os.environ.get('MINIO_ENDPOINT')
ACCESS_KEY=os.environ.get('MINIO_ROOT_USER')
SECRET_KEY=os.environ.get('MINIO_ROOT_PASSWORD')
BUCKET_NAME = os.environ.get('FASTAPI_BUCKET_NAME')
LOCAL_ROOT_PATH=os.environ.get('LOCAL_ROOT_PATH')


# 容器内部的挂载路径 (对应宿主机 D:\erp)
CONTAINER_ROOT_PATH = os.environ.get('CONTAINER_ROOT_PATH')
SAVE_ROOT_PREFIX = os.environ.get('SAVE_ROOT_PREFIX')

# 实例化 MinIO 客户端的辅助函数
def get_minio_client():
    if not MINIO_ENDPOINT or not ACCESS_KEY or not SECRET_KEY:
        raise HTTPException(status_code=500, detail="MinIO 配置信息缺失。")
    
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )
    # 确保 bucket 存在
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    return client



## ----------------------------------------------------
## 路由 1: 上传单个文件
## ----------------------------------------------------

@router.post("/upload/file")
async def upload_single_file(file_path):
    """接收路径，上传单个文件到 MinIO"""
    relative_path = os.path.relpath(file_path, LOCAL_ROOT_PATH)
    # 构造容器内的绝对路径
    local_abs_path = os.path.join(CONTAINER_ROOT_PATH, relative_path)
    # 构造 MinIO 上的 Key
    remote_key = f"{SAVE_ROOT_PREFIX}/{relative_path}".replace("\\", "/")

    if not os.path.isfile(local_abs_path):
        raise HTTPException(
            status_code=400, 
            detail=f"路径不是有效的文件: {file_path}"
        )
    # 确保 bucket 存在，不存在就创建
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    # 上传文件
    client = get_minio_client()
    try:
        client.fput_object(BUCKET_NAME, remote_key, local_abs_path)
        print(f"✅ 上传成功: {local_abs_path} -> {BUCKET_NAME}/{remote_key}")
        return {
            "status": "success",
            "message": "文件上传成功",
            "minio_key": remote_key
        }
    except S3Error as e:
        print(f"❌ MinIO Upload Failed: {e}")
        raise HTTPException(status_code=500, detail=f"MinIO 上传失败: {e.code}")


## ----------------------------------------------------
## 路由 2: 上传整个文件夹 (递归)
## ----------------------------------------------------

@router.post("/upload/folder")
async def upload_folder_recursive(folder_path):
    """接收路径，递归上传整个文件夹到 MinIO"""
    relative_folder_path = os.path.relpath(folder_path, LOCAL_ROOT_PATH)
    container_folder_path = os.path.join(CONTAINER_ROOT_PATH, relative_folder_path)
    # 构造容器内的绝对路径
    if not os.path.isdir(container_folder_path):
        raise HTTPException(
            status_code=400, 
            detail=f"路径不是有效文件夹: {folder_path}"
        )
    # 确保 bucket 存在，不存在就创建
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    # 上传文件
    client = get_minio_client()
    upload_count = 0
    
    try:
        for dir_path, _, filenames in os.walk(container_folder_path):
            for file in filenames:
                local_path = os.path.join(dir_path, file)
                
                # 关键：以 'folder_path' 为基准计算相对路径
                relative_path = os.path.relpath(local_path, CONTAINER_ROOT_PATH)
                
                # 构建 MinIO 上的完整 key，包含原始文件夹名
                remote_key = f"{SAVE_ROOT_PREFIX}/{relative_path}".replace("\\", "/")
                
                client.fput_object(BUCKET_NAME, remote_key, local_path)
                print(f"✅ 上传成功: {local_path} -> {remote_key}")
                upload_count += 1
                
        return {
            "status": "success",
            "message": f"文件夹上传成功，共 {upload_count} 个文件",
            "remote_base_path": f"{BUCKET_NAME}/{SAVE_ROOT_PREFIX}/{relative_folder_path}"
        }
    except S3Error as e:
        print(f"❌ MinIO Upload Failed: {e}")
        raise HTTPException(status_code=500, detail=f"MinIO 上传失败: {e.code}")

@router.get("/upload")
async def upload_file_to_minio():


    return 0
