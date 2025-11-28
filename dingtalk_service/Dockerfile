# ~/PROJECTS/dingtalk_service/Dockerfile
FROM python:3.13-slim

# 设置容器内的工作目录
WORKDIR /app

# 复制 requirements.txt 到容器中
COPY requirements.txt .

# 安装 Python 依赖
# --no-cache-dir 减少镜像层中的缓存，减小最终镜像大小
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用程序代码和模板文件到容器中
COPY app ./app

# 为了未来扩展性，可以先暴露一个端口。
EXPOSE 8001

# 定义容器启动时运行的命令
CMD ["python", "app/main.py"]

