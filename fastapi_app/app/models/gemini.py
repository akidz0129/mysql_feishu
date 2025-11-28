from fastapi import APIRouter             # 导入路由模块
from app.core.db import get_connection         # 导入数据库连接函数
from fastapi import FastAPI , Request              # 引入 FastAPI 框架主类
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse,StreamingResponse
from app.core.db import get_connection         # 导入数据库连接函数

from google import genai
from google.genai import types
import os


router = APIRouter(prefix="/gemini", tags=["Gemini"])  # 创建路由，所有路由路径自动加 /gemini 前缀，方便统一管理接口路。给这些路由统一打上 Orders 标签，方便 API 文档分组。

templates = Jinja2Templates(directory="templates")
api_key=os.environ.get("GEMINI_API_KEY")

@router.post("",response_class=HTMLResponse)
async def read_orders(request: Request):
    print(api_key)
    client = genai.Client(api_key=api_key)
    form = await request.form()
    question = form.get("question")
    print(question)


    response = client.models.generate_content(
        model="gemini-2.5-flash", contents=question,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=0) # Disables thinking
        ),
    )
    print(response.text)
    # return templates.TemplateResponse("index.html",{"request": request})
    return templates.TemplateResponse("index.html", {"request": request, "name": 'gemini',"data": response.text})
    

@router.post("/chat")
async def read_orders(request: Request):
    print(api_key)
    client = genai.Client(api_key=api_key)
    form = await request.form()
    question = form.get("question")
    print(question)


    response = client.models.generate_content(
        model="gemini-2.5-flash", contents=question,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=0) # Disables thinking
        ),
    )
    return  {"data": response.text}

from google import genai
from google.genai import types
from PIL import Image
from io import BytesIO
import base64
client = genai.Client()
import tempfile
@router.post("/imagen")
async def generate_image(request: Request):
    form = await request.form()
    prompt = form.get("prompt")
    file = form.get("file")  # 上传的 binary 流
    contents_list = []
    # input_image = None
    if file and file.filename:
        # 2. 读取上传文件的原始字节流
        # 注意：使用 await file.read() 来获取字节流
        file_bytes = await file.read() 
        
        # 3. 构建内联数据（Inline Data）Part
        # 直接使用读取到的字节和文件的 MIME Type
        gen_file = types.Part(
            inline_data=types.Blob(
                # data 参数接受原始字节
                data=file_bytes, 
                # mime_type 接受文件的内容类型（例如 'image/jpeg'）
                mime_type=file.content_type 
            )
        )
        contents_list.insert(0, gen_file)
    if prompt:
        print(prompt)
        contents_list.append(prompt)
        
    # 3. 检查 contents_list 是否为空（必须至少有一个元素）
    if not contents_list:
        # 如果既没有图像也没有 prompt，则应该抛出错误或提供默认提示
        raise ValueError("Prompt or image input is required.")

    response = client.models.generate_content(
        model="gemini-2.5-flash-image-preview",
        # contents=[gen_file, prompt] 
        contents=contents_list 
    )

    output_image = None # 初始化为 None

# 尝试从响应中提取图像
    if response.candidates and response.candidates[0].content.parts:
        for part in response.candidates[0].content.parts:
            # 检查 part 是否包含内联数据（图像）
            if part.inline_data and part.inline_data.data:
                from PIL import Image
                import io
                
                # 从字节流中加载 PIL Image 对象
                output_image = Image.open(io.BytesIO(part.inline_data.data))
                break # 找到图像后退出循环

    # 核心修正：在第 131 行之前添加 None 检查
    if output_image is not None:
        # 第 131 行    
        buf = io.BytesIO()
        output_image.save(buf, format="PNG") 
        # ... 后续逻辑
        buf.seek(0)
    
    # 返回图像数据给 n8n
        return StreamingResponse(buf, media_type="image/png") 
    else:
        # 图像未找到或模型只返回了文本。
        # 您需要在这里处理文本响应，而不是抛出错误。
        return {"status": "success", "text": response.text} 
    

from typing import Optional
from fastapi import APIRouter, File, UploadFile, Form
@router.post("/multi-image")
async def generate_image(request: Request):
    """
    通过 Request 对象手动解析 multipart/form-data，实现简洁的函数签名。
    """
    # 1. 解析整个表单
    form = await request.form()
    
    # 2. 提取所有输入
    prompt = form.get("prompt")
    print(prompt)
    file1 = form.get("file1") # 获取 UploadFile 或 None
    file2 = form.get("file2") # 获取 UploadFile 或 None
    contents_list = []

    # --- 1. 处理第一张图片 (file1) ---
    if file1 and file1.filename:
        # 读取字节流
        file1_bytes = await file1.read()
        
        # 确保文件内容不为空
        if file1_bytes:
            part1 = types.Part(
                inline_data=types.Blob(
                    data=file1_bytes, 
                    mime_type=file1.content_type
                )
            )
            contents_list.append(part1)

    # --- 2. 处理第二张图片 (file2) ---
    if file2 and file2.filename:
        # 读取字节流
        file2_bytes = await file2.read()
        
        # 确保文件内容不为空
        if file2_bytes:
            part2 = types.Part(
                inline_data=types.Blob(
                    data=file2_bytes, 
                    mime_type=file2.content_type
                )
            )
            contents_list.append(part2)

    # --- 3. 添加文本 Prompt ---
    if prompt:
        contents_list.append(prompt)
        
    # --- 4. 健壮性检查 ---
    if not contents_list:
        return {"error": "缺少 Prompt 或至少一张图片输入"}, 400

    # --- 5. 调用 Gemini API ---
    response = client.models.generate_content(
        model="gemini-2.5-flash-image-preview", # 请确保此模型支持您的多图任务
        contents=contents_list 
    )
    output_image = None # 初始化为 None

# 尝试从响应中提取图像
    if response.candidates and response.candidates[0].content.parts:
        for part in response.candidates[0].content.parts:
            # 检查 part 是否包含内联数据（图像）
            if part.inline_data and part.inline_data.data:
                from PIL import Image
                import io
                
                # 从字节流中加载 PIL Image 对象
                output_image = Image.open(io.BytesIO(part.inline_data.data))
                break # 找到图像后退出循环

    # 核心修正：在第 131 行之前添加 None 检查
    if output_image is not None:
        buf = io.BytesIO()
        output_image.save(buf, format="PNG") 
        buf.seek(0)
    
    # 返回图像数据给 n8n
        return StreamingResponse(buf, media_type="image/png") 
    else:
        # 图像未找到或模型只返回了文本。
        # 您需要在这里处理文本响应，而不是抛出错误。
        return {"status": "success", "text": response.text} 

async def classify_user_intent(user_input: str) -> str:
    """
    使用 Gemini API 判断用户输入的意图。
    Args:
        user_input: 用户输入的文本。
    Returns:
        意图类型字符串，例如 "OrderQuery", "GeneralQuestion", "Unknown"。
    """
    try:
        client = genai.Client(api_key=api_key)
        
        prompt = f"""你是一个意图分类助手。请根据用户的问题，判断其意图属于以下哪种类型：
            - "OrderQuery" (订单查询)
            - "GeneralQuestion" (通用问题)
            - "Unknown" (未知意图)

            请直接输出意图类型，不要有其他解释或多余的文字。

            用户问题：{user_input}
            意图："""

        response = client.models.generate_content(
        model="gemini-2.5-flash", contents=prompt,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=0) # Disables thinking
        ),
    )
        # 尝试从响应中获取文本内容
        
        intent = response.text.strip()
        print(intent)
        # 验证返回的意图是否在预期的列表中
        if intent in ["OrderQuery", "GeneralQuestion", "Unknown"]:
            return intent
        else:
            # 如果 Gemini 返回了意料之外的格式或内容
            return "Unknown"
            
    except Exception as e:
        print(f"调用 Gemini API 失败或发生错误: {e}")
        return "Unknown" # 出错时默认为未知意图
    
async def _extract_order_id_with_ai(self, user_input: str) -> str | None:
        """
        尝试从用户输入文本中提取一个符合 ASIN 模式的字符串。
        假设 ASIN 是 10 位由大写字母和数字组成的字符串。
        """
        try:
            client = genai.Client(api_key=api_key)

                # 精心设计的提示词，明确指示 AI 提取目标信息和返回格式
            prompt = f"""从以下用户输入的文本中，准确提取**唯一的订单号**。
                订单号通常是10位由大写字母和数字组成的字符串（例如：B0D1FRQX8Q）。
                如果文本中包含多个订单号，只返回第一个。
                如果文本中**不包含任何符合格式的订单号**，请明确返回字符串 "**NONE**"。
                除了提取的订单号或"NONE"，不要包含任何其他解释或文字。

                用户输入: {user_input}
                提取的订单号:"""
            response = client.models.generate_content(
                    model="gemini-2.5-flash", # 选择合适的模型
                    contents=prompt,
                    # thinking_config=types.ThinkingConfig(thinking_budget=0) # 建议移除或注释此行，让模型有少量“思考”时间，可能提高准确性
                )

            extracted_id = response.text.strip()
            self.logger.info(f"AI 订单号提取结果: '{extracted_id}' (原始输入: {user_input})")

            if extracted_id.upper() == "NONE":
                return None
            import re
            if re.fullmatch(r'[A-Z0-9]{10}', extracted_id):
                    return extracted_id
            else:
                    self.logger.warning(f"AI 提取的订单号 '{extracted_id}' 不符合预期格式，可能不准确。")
                    return None # AI 提取的格式不符，视为未提取到有效 ID
        except Exception as e:
            self.logger.error(f"调用 Gemini API 提取订单号失败: {e}")
            return None # 发生错误时返回 None