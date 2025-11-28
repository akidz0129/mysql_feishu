# Dockerfile
FROM apache/spark:3.5.6

USER root


# 复制 requirements.txt 到容器中
COPY requirements.txt .


RUN pip install --no-cache-dir --index-url https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

USER spark

WORKDIR /app
# 复制 PySpark 应用程序
COPY app ./app





# 明确清除基础镜像可能带有的 ENTRYPOINT
# 这将确保后续的 CMD 独立执行，而不是作为 ENTRYPOINT 的参数
ENTRYPOINT []

# 注意：这个容器不会自动启动，你需要手动 exec 进去运行 spark-submit
# 或者通过一个 shell 脚本在后台自动触发
# CMD ["tail", "-f", "/dev/null"] 