import os
import subprocess
import threading
import time
import re
import asyncio
import json
import httpx
from pydantic import BaseModel, Field
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi import APIRouter, Request, HTTPException
import os


router = APIRouter(prefix="/cf", tags=["CloudFlare"])  # 创建路由，所有路由路径自动加前缀，方便统一管理接口路。给这些路由统一打上标签，方便 API 文档分组。

templates = Jinja2Templates(directory="templates")

# --- 1. 配置和全局状态管理 ---

# 从环境变量获取配置
CF_BIN_PATH = os.environ.get('CF_BIN_PATH')
N8N_INTERNAL_URL = os.environ.get('N8N_INTERNAL_URL')
# 假设钉钉 Webhook URL 存储在环境变量中
DINGTALK_WEBHOOK = os.environ.get('DINGTALK_WEBHOOK') 

# 全局变量来管理进程和存储动态 URL
tunnel_process: subprocess.Popen | None = None
current_public_url: str | None = None
tunnel_lock = threading.Lock() 
URL_PATTERN = re.compile(r'https?://[a-z0-9-]+\.trycloudflare\.com')


# --- 3. Tunnel 进程管理函数 (在单独的线程中运行) ---


# --- 4. FastAPI Webhook 路由 ---



# 接口模型定义 (已在上方给出)
class N8nStatusData(BaseModel):
    status: str = Field(..., example="running")
    url: str = Field(..., example="https://abc-xyz.trycloudflare.com")
    message: str = Field(..., example="N8N 临时隧道已启动。")

class N8nApiResponse(BaseModel):
    code: int = Field(200)
    data: N8nStatusData
# [CF Log] 用于调试 cloudflared 进程的实际输出

def start_and_capture_tunnel():
    global tunnel_process, current_public_url
    
    command = [
        CF_BIN_PATH,
        "tunnel", 
        "--url", 
        N8N_INTERNAL_URL
    ]
    # 🚨 新增调试日志
    print(f"[TUNNEL DEBUG] Attempting to run command: {' '.join(command)}") 
    
    current_public_url = None
    
    try:
        tunnel_process = subprocess.Popen(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True
        )
        print(f"[TUNNEL DEBUG] Process started with PID: {tunnel_process.pid}") 
        
        # 实时读取输出
        for line in iter(tunnel_process.stdout.readline, ''):
            line = line.strip()
            print(f"[CF Log] {line}") # 确保这行打印了 cloudflared 的所有输出
            
            match = URL_PATTERN.search(line)
            if match:
                with tunnel_lock:
                    current_public_url = match.group(0)
                print(f"[TUNNEL DEBUG] SUCCESS! Captured URL: {current_public_url}")
                # 找到 URL 后，继续循环让进程保持运行
        
        # 🚨 进程退出后的错误报告
        return_code = tunnel_process.wait()
        print(f"[TUNNEL ERROR] Process terminated unexpectedly. Code: {return_code}") 
        
    except FileNotFoundError:
        # 🚨 确保这个错误能被记录
        error_msg = f"错误: 找不到 Cloudflared 执行文件在 {CF_BIN_PATH}"
        print(error_msg)
    except Exception as e:
        error_msg = f"临时隧道运行错误: {e}"
        print(error_msg)
        
    finally:
        with tunnel_lock:
            # 清理状态，确保 current_public_url 被清空
            tunnel_process = None
            current_public_url = None # 确保如果失败，状态是 None

@router.get("/n8n-tunnel/{action}", response_model=N8nApiResponse) # 🚨 加上 response_model
async def control_n8n_tunnel(action: str):
    global tunnel_process, current_public_url
    
    action = action.strip().lower()
    status_data = N8nStatusData(status="error", url="", message="内部错误")
    
    with tunnel_lock:
        is_running = tunnel_process and tunnel_process.poll() is None
        
        # --- START Action ---
        if action == "start":
            if is_running:
                # 状态 1: 已经运行，直接返回 URL
                status_data.status = "running"
                status_data.url = current_public_url or "URL_LOST"
                status_data.message = f"N8N 隧道已在运行。URL: {current_public_url}"
                
            elif current_public_url:
                 # 状态 2: 进程可能已经退出，但 URL 还在全局变量里 (理论上不该发生，但作为安全检查)
                status_data.status = "stale_running"
                status_data.url = current_public_url
                status_data.message = "隧道状态异常，但URL已捕获。请发送'n8n stop'后重试。"
            
            else:
                # 状态 3: 未运行，执行启动，并立即返回“正在启动”
                threading.Thread(target=start_and_capture_tunnel, daemon=True).start()
                
                # 🚨 关键变化：只等待 3 秒进行快速确认
                url_ready = False
                for _ in range(3): 
                    if current_public_url:
                        url_ready = True
                        break
                    await asyncio.sleep(1) 

                if url_ready:
                    # 快速启动成功，直接返回 URL
                    status_data.status = "started"
                    status_data.url = current_public_url
                    status_data.message = f"N8N 隧道启动成功（快速响应）。"
                else:
                    # 未在 3 秒内启动，返回“正在启动”
                    status_data.status = "starting"
                    status_data.message = "N8N 隧道已触发启动。请稍候再次发送 'n8n start' 获取链接。"
        
        # --- STOP Action / Invalid Action (保持不变) ---
        elif action == "stop":
            # ... (停止逻辑保持不变，因为停止操作非常快，不会超时) ...
            if not is_running:
                status_data.status = "inactive"
                status_data.message = "N8N 隧道未运行。"
            else:
                tunnel_process.terminate() 
                tunnel_process = None
                current_public_url = None
                status_data.status = "stopped"
                status_data.url = ""
                status_data.message = "N8N 隧道已断开连接，URL 已失效。"
        
        else:
            raise HTTPException(status_code=400, detail="无效指令。请使用 'start' 或 'stop'。")
            
    # 构造最终响应
    return N8nApiResponse(
        code=200, 
        data=status_data
    )