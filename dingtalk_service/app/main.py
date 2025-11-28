import argparse
import logging
import json
import datetime
from dingtalk_stream import AckMessage, CallbackMessage, ChatbotMessage, Credential, DingTalkStreamClient, ChatbotHandler
import os
import httpx
import re
import yaml

# --- 模块级别日志设置（优先执行） ---
# 获取当前模块的 logger 实例
logger = logging.getLogger(__name__) 
# 创建一个 StreamHandler，将日志输出到控制台
handler = logging.StreamHandler()
# 设置日志格式
formatter = logging.Formatter('%(asctime)s %(name)-8s %(levelname)-8s %(message)s [%(filename)s:%(lineno)d]')
handler.setFormatter(formatter)
# 为 logger 添加处理器
logger.addHandler(handler)
# 设置日志级别（例如 INFO，这样 INFO, WARNING, ERROR, CRITICAL 都会显示）
logger.setLevel(logging.INFO)

# --- 其他全局变量 ---
APP_ID = os.environ.get("DINGTALK_APP_ID")
APP_SECRET = os.environ.get("DINGTALK_APP_SECRET")

class AppSettings:
    field_mappings: dict = {}
    # 可以添加其他配置项

    def load_from_yaml(self):
        logger = logging.getLogger(__name__)
        logger.info("正在尝试加载 YAML 配置文件...")
        # 从环境变量获取配置文件路径，如果未设置则使用默认路径
        file_path = os.getenv("CONFIG_FILE_PATH", "/app/config/mappings.yml") 
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
                self.field_mappings = config_data.get('field_mappings', {})
                logger.info(f"成功从 {file_path} 加载映射配置。")
        except FileNotFoundError:
            logger.info(f"警告：配置文件未找到于 {file_path}。使用空映射。")
            self.field_mappings = {}
        except yaml.YAMLError as e:
            logger.info(f"错误：解析 YAML 配置文件 {file_path} 失败：{e}。使用空映射。")
            self.field_mappings = {}

# 在应用启动时加载配置
settings = AppSettings()
settings.load_from_yaml()

# 您现在可以通过 settings.field_mappings 访问映射数据
FIELD_MAPPING = settings.field_mappings 


def define_options():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--msg', dest='msg', default='python-getting-start say：hello',
        help='要发送的消息内容'
    )
    return parser.parse_args()

class DingTalkMessageHandler(ChatbotHandler):
    def __init__(self, logger: logging.Logger, client_id: str, client_secret: str, robot_code: str = None):
        super(ChatbotHandler, self).__init__()  
        self.logger = logger
        self.client_id = client_id
        self.client_secret = client_secret
        self.robot_code = robot_code
        self.fastapi_order_url = "http://fastapi_app:8000/orders" 
        self.fastapi_n8n_url = "http://fastapi_app:8000/cf/n8n-tunnel" 

    async def process(self, callback: CallbackMessage):
        try:
            # 解析钉钉消息
            incoming_message = ChatbotMessage.from_dict(callback.data)
            user_id = incoming_message.sender_staff_id
            
            # 获取用户发送的文本内容
            user_text_content = ""
            if incoming_message.text and incoming_message.text.content:
                user_text_content = incoming_message.text.content.strip()
            else:
                user_text_content = "非文本消息，或内容为空"
                self.logger.info(f"收到非文本消息: {json.dumps(callback.data, indent=4)}")

            self.logger.info(f"收到消息: {user_text_content}")

            # 生成回复内容
            reply_content = await self.generate_reply(user_text_content)
            
            # 通过Stream客户端回复消息
            self.reply_text(reply_content, incoming_message)
            self.logger.info(f"成功回复: {reply_content}")
            
            # 可选：如果需要发送私聊消息，可以调用下面的代码
            # if self.robot_code:
            #     access_token = get_token(self.client_id, self.client_secret)
            #     if access_token:
            #         send_robot_private_message(access_token, self.robot_code, reply_content, [user_id])
            
            return AckMessage.STATUS_OK, 'OK'
            
        except Exception as e:
            self.logger.error(f"处理消息失败: {e}")
            # 修复：使用正确的状态常量
            return AckMessage.STATUS_SYSTEM_EXCEPTION, f"Failed to process: {e}"
        
    async def generate_reply(self, user_input: str) -> str:
        """
        根据用户输入生成回复内容
        """
        # 将用户输入转为大写并去除首尾空白，便于统一匹配
        normalized_input = user_input.strip().upper()
        reply_content = f"收到你的消息: {user_input}\n"
        

            # 进行映射，如果找不到映射，可以保持原样或忽略
        # requested_db_fields = []
        # for field_term in requested_fields_raw:
            # mapped_field = FIELD_MAPPING.get(field_term.lower(), field_term) # 尝试小写匹配
            # requested_db_fields.append(mapped_field)
        # reply_content+=str(requested_db_fields)
        # reply_content+=str(mapped_field)
        
        # 优先级1: 简单的关键词回复，避免不必要的API调用
        if normalized_input in ["帮助", "HELP"]:
            return """我可以为您提供以下服务:
- 特定日期日报: 请尝试格式如 “#日报#YYYY-MM-DD#”
- 特定日期特定产品销量:  请尝试格式如 “#销量#YYYY-MM-DD#asin#”
- 特定日期特定产品范围销量: 请尝试格式如 “#销量#YYYY-MM-DD(起始时间)#YYYY-MM-DD(结束时间)#asin#”
- 特定日期销量排名: 请尝试格式如 “#排名#YYYY-MM-DD#”
- 特定日期范围销量排名: 请尝试格式如 “#排名#YYYY-MM-DD(起始时间)#YYYY-MM-DD(结束时间)#”
如果您有其他需求，也可以尝试告诉我。
"""

        

        logger.info(normalized_input)




        if normalized_input =="SHOPEE":
            import hmac
            import time
            import hashlib
            partner_id=2012183
            host="https://partner.shopeemobile.com/"
            path = "/api/v2/shop/auth_partner"
            redirect_url = "https://www.baidu.com/"
            tmp = "shpk6a5747795a784f466c42455a4162586f6f644b63597855574b524c577868"
            timest = int(time.time())
            partner_key = tmp.encode()
            tmp_base_string = "%s%s%s" % (partner_id, path, timest)
            base_string = tmp_base_string.encode()
            sign = hmac.new(partner_key, base_string, hashlib.sha256).hexdigest()
            ##generate api
            url = host + path + "?partner_id=%s&timestamp=%s&sign=%s&redirect=%s" % (partner_id, timest, sign, redirect_url)
            return url
        n8n_match=re.match(r"n8n (.*)$", user_input.strip(), re.IGNORECASE)
        if n8n_match:
            action=n8n_match.group(1)
                        # 使用 httpx.AsyncClient 发送异步 HTTP 请求到你的 FastAPI 接口
            async with httpx.AsyncClient() as client:
                
                response = await client.get(f"{self.fastapi_n8n_url}/{action}")
                response.raise_for_status() # 检查 HTTP 状态码，如果不是 2xx 则抛出 httpx.HTTPStatusError
                dat = response.json()
            
            # 安全地从 JSON 响应中获取数据，提供默认值以防键不存在
            order_count = dat.get("data", {}).get("order_count", 0)
            total_sale = dat.get("data", {}).get("total_sale", 0.0)

            return (
                f"查询结果：订单数量 {order_count}，总销售额 {total_sale}。\n"
                f"感谢您的使用，发送'帮助'查看更多功能。"
            )
            return url
# 匹配意图
        date_error="日期格式不正确。请使用 YYYY-MM-DD 格式，例如：#2025-06-01#"
        detail_date_report_match=re.match(r"^#\s*日报#(\d{4}-\d{1,2}-\d{1,2})#(.*)$", user_input.strip(), re.IGNORECASE)
        if detail_date_report_match:
            try:
                date_str = detail_date_report_match.group(1)
                remaining_content = detail_date_report_match.group(2)
                parts = remaining_content.split('#')
                
                requested_fields_raw = [part for part in parts[:-1] if part]
                requested_db_fields = []
                for field_term in requested_fields_raw:
                    mapped_field = FIELD_MAPPING.get(field_term.lower(), field_term) # 尝试小写匹配
                    requested_db_fields.append(mapped_field)
                     # 调用 FastAPI 订单服务
                response_message = await self._query_daily_report(
                # report_date=report_date,
                # columns=db_columns_to_select,
                report_date=date_str,
                columns=requested_db_fields,
                )
                



                date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                info = f"用户请求生成{date}日报。"
            except ValueError:
                return date_error
            self.logger.info(info)
            # return reply_content + info
            return response_message


        date_report_match =re.match(r"^#\s*日报#(\d{4}-\d{1,2}-\d{1,2})#", user_input.strip(), re.IGNORECASE)
        if date_report_match:
            try:
                date_str = date_report_match.group(1)
                date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                info = f"用户请求生成{date}日报。"
            except ValueError:
                return date_error
            self.logger.info(info)
            return reply_content + info
        
        sale_match =re.match(r"^#\s*销量#(\d{4}-\d{1,2}-\d{1,2})#(\d{4}-\d{1,2}-\d{1,2})#([A-Z0-9]{10})#", user_input.strip(), re.IGNORECASE)
        if sale_match:
            try:
                date_str1 = sale_match.group(1)
                date_str2 = sale_match.group(2)
                asin_str = sale_match.group(3)
                date1 = datetime.datetime.strptime(date_str1, "%Y-%m-%d").date()
                date2 = datetime.datetime.strptime(date_str2, "%Y-%m-%d").date()
                info=f"用户请求生成{date1}到{date2} {asin_str}销量。"
            except ValueError:
                return date_error
            self.logger.info(info)
            return reply_content + info
        
        date_sale_match =re.match(r"^#\s*销量#(\d{4}-\d{1,2}-\d{1,2})#([A-Z0-9]{10})#", user_input.strip(), re.IGNORECASE)
        if date_sale_match:
            try:
                date_str = date_sale_match.group(1)
                asin_str = date_sale_match.group(2)
                date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                info=f"用户请求生成{date} {asin_str}销量。"
            except ValueError:
                return date_error
            self.logger.info(info)
            return reply_content + info
        
        ranking_match =re.match(r"^#\s*排名#(\d{4}-\d{1,2}-\d{1,2})#(\d{4}-\d{1,2}-\d{1,2})#", user_input.strip(), re.IGNORECASE)
        if ranking_match:
            try:
                date_str1 = ranking_match.group(1)
                date_str2 = ranking_match.group(2)
                date1 = datetime.datetime.strptime(date_str1, "%Y-%m-%d").date()
                date2 = datetime.datetime.strptime(date_str2, "%Y-%m-%d").date()
                info = f"用户请求生成{date1}到{date2}排名。"
            except ValueError:
                return date_error
            self.logger.info(info)
            return reply_content + info
        
        date_ranking_match =re.match(r"^#\s*排名#(\d{4}-\d{1,2}-\d{1,2})#", user_input.strip(), re.IGNORECASE)
        if date_ranking_match:
            try:
                date_str = date_ranking_match.group(1)
                date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                info = f"用户请求生成{date}排名。"
            except ValueError:
                return date_error
            self.logger.info(info)
            return reply_content + info
        
        return reply_content+"听不懂"
        
        #     # --- 优先级2: Gemini 意图判断 ---
        # intent = await self.classify_user_intent(user_input) # 调用 Gemini 进行意图识别
        
        # if intent == "OrderQuery":
        #     # --- 订单查询逻辑 ---
        #     return await self._handle_order_query(user_input)
 
    async def _handle_order_query(self, user_input: str) -> str:
        """
        处理订单查询意图。
        """
        # 尝试从用户输入中提取 ASIN/订单号
        extracted_asin = await self._extract_order_id_with_ai(user_input)
        
        if not extracted_asin:
            return "请提供具体的订单号以便我进行查询，例如：查询订单 B0D1FRQX8Q"

        try:
            # 使用 httpx.AsyncClient 发送异步 HTTP 请求到你的 FastAPI 接口
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.fastapi_order_url}?name={extracted_asin}")
                response.raise_for_status() # 检查 HTTP 状态码，如果不是 2xx 则抛出 httpx.HTTPStatusError
                dat = response.json()
            
            # 安全地从 JSON 响应中获取数据，提供默认值以防键不存在
            order_count = dat.get("data", {}).get("order_count", 0)
            total_sale = dat.get("data", {}).get("total_sale", 0.0)

            return (
                f"正在查询订单号 '{extracted_asin}'。\n"
                f"查询结果：订单数量 {order_count}，总销售额 {total_sale}。\n"
                f"感谢您的使用，发送'帮助'查看更多功能。"
            )
        except httpx.HTTPStatusError as e:
            self.logger.error(f"查询订单时 HTTP 错误: {e.response.status_code} - {e.response.text}")
            return f"查询订单失败，服务器返回错误：{e.response.status_code}。请稍后再试。"
        except httpx.RequestError as e:
            self.logger.error(f"查询订单时请求错误: {e}")
            return "查询订单失败，无法连接到查询服务。请检查服务状态。"
        except Exception as e:
            self.logger.exception(f"处理订单查询时发生未知错误: {e}")
            return "查询订单时发生未知错误，请联系管理员。"
    
    async def _query_daily_report(self, report_date: str, columns: list[str]) -> str:
        """
        向 FastAPI 服务发送日报查询请求。
        """
        payload = {
            "report_date": report_date,
            "columns": columns,
        }
        self.logger.info(f"正在向 FastAPI 发送日报查询请求: {payload}")
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{self.fastapi_order_url}/report", json=payload, timeout=30.0)
                response.raise_for_status() # 检查 HTTP 状态码

                fastapi_response_data = response.json()
                # 假设 FastAPI 返回的数据结构类似 {"status": "success", "data": [...]}
                if fastapi_response_data.get("status") == "success":
                    report_data = fastapi_response_data.get("data", [])
                    if report_data:
                        # 格式化查询结果为可读的字符串
                        # 这是一个简化的例子，您可能需要更复杂的格式化
                        formatted_report = f"{report_date}日报结果:\n"
                        for row in report_data:
                            formatted_report += f"- {row}\n" # 假设 row 是一个字典或列表
                        return formatted_report
                    else:
                        return f"未找到 {report_date} 的日报数据。"
                else:
                    error_msg = fastapi_response_data.get("message", "未知错误")
                    self.logger.error(f"FastAPI 返回错误: {error_msg}")
                    return f"日报查询失败: {error_msg}"
        except httpx.HTTPStatusError as e:
            self.logger.error(f"查询日报时 HTTP 错误: {e.response.status_code} - {e.response.text}")
            return f"查询日报失败，服务器返回错误：{e.response.status_code}。请稍后再试。"
        except httpx.RequestError as e:
            self.logger.error(f"查询日报时请求错误: {e}")
            return "查询日报失败，无法连接到查询服务。请检查服务状态和FASTAPI_ORDER_URL。"
        except Exception as e:
            self.logger.exception(f"处理日报查询时发生未知错误: {e}")
            return "查询日报时发生未知错误，请联系管理员。"
def main():
    options = define_options()

 # 优先从环境变量获取，如果不存在再尝试命令行参数
    client_id = os.getenv("DINGTALK_APP_ID", APP_ID)
    client_secret = os.getenv("DINGTALK_APP_SECRET", APP_SECRET)
    robot_code = client_id


    if not client_id or not client_secret:
        logger.error("请提供 client_id 和 client_secret。可以通过命令行参数或修改代码中的APP_ID/APP_SECRET。")
        return

    # 创建钉钉Stream客户端
    credential = Credential(client_id, client_secret)
    client = DingTalkStreamClient(credential)  # 移除logger参数，避免潜在问题

    # 注册消息处理器
    handler = DingTalkMessageHandler(logger, client_id, client_secret, robot_code)
    client.register_callback_handler(ChatbotMessage.TOPIC, handler)

    logger.info("钉钉机器人 Stream 模式客户端启动中...")
    client.start_forever()

if __name__ == '__main__':
    main()