# Dockerfile
# 使用 mambaforge 基础镜像
FROM condaforge/mambaforge:latest

# 设置容器内的工作目录
WORKDIR /app

# 复制 environment.yml 到容器中，以便安装 Conda 环境
COPY environment.yml .

# 使用 mamba 创建 Conda 环境并安装所有依赖
# 如果你决定通过 pip 来安装 requirements.txt，确保 requirements.txt 在 environment.yml 之前被 COPY
# 如果 environment.yml 包含 pip: -r requirements.txt，则不需要额外 COPY requirements.txt
RUN mamba env create -f environment.yml && \
    conda clean --all --yes

    
# 激活 Conda 环境
ENV CONDA_DEFAULT_ENV fastapi_env
ENV PATH /opt/conda/envs/$CONDA_DEFAULT_ENV/bin:$PATH

# 复制应用程序代码和模板文件到容器中
# 根据你的新文件结构：app/ 和 templates/ 都在根目录下
COPY app ./app
COPY templates ./templates

# 暴露 FastAPI 应用程序将监听的端口
EXPOSE 8000

# 定义容器启动时运行的命令
# 你的 FastAPI 应用入口是 app/main.py 中的 app 对象
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

