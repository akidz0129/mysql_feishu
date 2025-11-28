# /app/app/create_table.py

import os
from pyspark.sql import SparkSession
import logging
import time
import socket # 新增导入
from pyspark.sql import SparkSession
import logging # 导入 logging 模块，如果之前没有
from common_utils import get_spark_and_s3_clients,setup_logging,clean_currency,clean_region,to_datetime_aware

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
setup_logging()
# 获取 SQL 文件路径
# 假设 SQL 文件也在 /app/app 目录下，或者你指定一个子目录
SQL_FILE_PATH = "/app/app/create_iceberg_tables.sql" # 请确保这个路径正确

def create_iceberg_tables(spark: SparkSession, sql_file: str):
    """
    读取 SQL 文件并执行其中的 DDL 语句来创建 Iceberg 表。
    """
    logger.info(f"正在尝试创建 Iceberg 表，SQL 文件路径: {sql_file}")

    if not os.path.exists(sql_file):
        logger.error(f"错误：SQL 文件不存在于 {sql_file}")
        raise FileNotFoundError(f"SQL 文件 {sql_file} 不存在。")

    try:
    
        with open(sql_file, 'r') as f:
            sql_commands = f.read()

        # 将 SQL 命令按分号分割，并过滤掉空行
        # 注意：这里简单的分割可能不适用于包含分号的复杂 SQL 语句（如存储过程或带分号的字符串文字）
        # 对于简单的 DDL 语句通常够用
        commands = [cmd.strip() for cmd in sql_commands.split(';') if cmd.strip()]

        # 获取 Iceberg Catalog 的名称
        # 默认使用 spark.sql.catalog.iceberg_mysql_catalog
        # 可以在 spark-defaults.conf 中设置 spark.sql.defaultCatalog
        catalog_name = spark.conf.get("spark.sql.defaultCatalog", "iceberg_mysql_catalog") # 假设你的 catalog 名叫 iceberg_mysql_catalog

        # 尝试设置默认 Catalog，如果已设置则会忽略
        try:
            spark.sql(f"USE {catalog_name};")
            logger.info(f"已设置默认 Iceberg Catalog 为: {catalog_name}")
        except Exception as e:
            logger.warning(f"无法设置默认 Catalog {catalog_name}，可能是因为它不是 Hive/JDBC Catalog 或者已经设置过。错误: {e}")
            # 如果不能设置默认 Catalog，那么 SQL 语句中的数据库名需要包含 Catalog 前缀
            # 例如：CREATE TABLE iceberg_mysql_catalog.default_iceberg_db.total_orders
            logger.warning("请确保您的 SQL 语句包含了完整的 Catalog 和数据库路径，例如: `USE iceberg_mysql_catalog.default_iceberg_db;`")


        for i, command in enumerate(commands):
            if command: # 再次检查确保不是空命令
                logger.info(f"正在执行 SQL 命令 {i+1}: ")
                # logger.info(f"正在执行 SQL 命令 {i+1}: \n{command}")
                spark.sql(command)
                logger.info(f"SQL 命令 {i+1} 执行成功。")

        logger.info("所有 Iceberg 表已成功创建/检查。")

    except Exception as e:
        logger.error(f"创建 Iceberg 表时发生错误: {e}", exc_info=True)
        raise # 重新抛出异常，让 Spark 任务失败

if __name__ == "__main__":
    spark, s3_client = get_spark_and_s3_clients(app_name="createTable",enable_iceberg=True)
    # minio_host = "minio"
    # minio_ip = None
    # logger.info("正在尝试解析 MinIO IP 地址...")
    # try:
    #     # 尝试解析MinIO的主机名到IP地址
    #     minio_ip = socket.gethostbyname(minio_host)
    #     logger.info(f"已解析 MinIO IP: {minio_ip}")
    # except socket.gaierror as e:
    #     logger.warning(f"解析 MinIO 主机名 '{minio_host}' 失败: {e}. 等待并重试...")
    #     # 如果解析失败，可能是MinIO服务还没完全启动或网络未就绪，等待并重试
    #     for i in range(1, 11): # 尝试10次
    #         time.sleep(2)
    #         try:
    #             minio_ip = socket.gethostbyname(minio_host)
    #             logger.info(f"第 {i} 次重试成功，已解析 MinIO IP: {minio_ip}")
    #             break
    #         except socket.gaierror:
    #             logger.warning(f"第 {i} 次重试失败，继续等待...")
    #     if not minio_ip:
    #         logger.error(f"多次重试后仍无法解析 MinIO 主机 '{minio_host}'。任务失败。")
    #         raise Exception(f"无法解析 MinIO 主机 '{minio_host}'。")
    # logger.info("--- Spark 配置及环境变量检查开始 ---")
    # spark = SparkSession.builder.config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_ip}:9000") \
    #     .config("spark.sql.catalog.iceberg_mysql_catalog.s3.endpoint", f"http://{minio_ip}:9000") \
    #         .getOrCreate()


    try:
        # 调用函数执行 SQL
        create_iceberg_tables(spark, SQL_FILE_PATH)
    finally:
        # 停止 SparkSession
        spark.stop()
        logger.info("SparkSession 已停止。")