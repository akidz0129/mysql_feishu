import uuid
from common_utils import get_spark_and_s3_clients,setup_logging,clean_currency,clean_region,to_datetime_aware
from common_utils import to_datetime,to_decimal,to_nullable_bool,to_nullable_int,to_nullable_string, get_iso_country_code_pycountry,get_minio_ip
from pyspark.sql.functions import to_timestamp, col
import logging
import os
import io
import pandas as pd
import numpy as np
from _config import FIELD_MAPPING_BY_COUNTRY,DB_CONFIG,ALLOWED_DB_COLUMNS,RAW_DATA_BUCKET, UNCLASSIFIED_ROOT_PREFIX, CLASSIFIED_PREFIX, UNIDENTIFIED_RAW_PREFIX,DATETIME_COLUMNS_CLEANERS
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DecimalType, DoubleType, IntegerType, BooleanType
)
from decimal import Decimal # 导入 Decimal 类型
from pyspark.sql.functions import lit
import shutil
import os
# # 配置 MinIO 访问
import os
import glob
import shutil


# --- SparkSession 初始化 ---
setup_logging()

spark, s3_client = get_spark_and_s3_clients(app_name="Extracter",enable_iceberg=True)



bucket = "jiaoji"
# 创建一个临时目录用于存放原始 CSV 输出
temp_output_path = f"s3a://{bucket}/output_csv/"
# 最终输出文件的路径，指定了完整文件名
final_output_path = f"s3a://{bucket}/output_c/final_output.csv"

output_path = f"s3a://{bucket}/output_csv/"
# 读 Iceberg 表
table_name = "iceberg_mysql_catalog.total_orders_db.total_orders"
# print(f"正在从 Iceberg 表 {table_name} 读取数据...")

# df = spark.read.format("iceberg").load(table_name)

#    # --- 写成 CSV 到 MinIO桶的临时目录 ---
#     # 使用 coalesce(1) 确保只生成一个 part-*.csv 文件
#     # 使用 mode("overwrite") 覆盖之前可能存在的临时文件
# print(f"正在将数据写入临时目录: {temp_output_path}")
# df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)

# # --- 找到生成的 CSV 文件并添加 UTF-8 BOM ---
# print("\n正在处理生成的 CSV 文件...")
# # 获取临时目录下的所有对象
objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=os.path.basename(temp_output_path) + "/")
    
part_file_key=None

paginator = s3_client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket, Prefix="output_csv")

files_to_process = []
for page in pages:
    for obj in page.get('Contents', []):
        # if obj['Key'].endswith('.csv') and obj['Key'].startswith(os.path.basename(temp_output_path) + "/part-"):
        if obj['Key'].endswith('.csv') and obj['Key'].startswith("output_csv/part-"):
            print(1)
            part_file_key = obj['Key']
            break
        # if not obj['Key'].endswith('/'): 
        #     files_to_process.append(obj['Key'])
        #     print(obj['Key'])
# for object_key in files_to_process:
#     print(f"\n正在处理文件: {object_key}")
#     file_name = os.path.basename(object_key)
#     if file_name.startswith("part-") and file_name.endswith(".csv"):
#         part_file_path = f"s3a://{bucket}/{object_key}"
#         break
if part_file_key:
    print(f"找到 CSV 文件: {part_file_key}")
    
    # 从 S3/MinIO 读取文件内容
    print("正在读取文件内容并添加 UTF-8 BOM...")
    response = s3_client.get_object(Bucket=bucket, Key=part_file_key)
    # 将原始数据解码为 UTF-8 字符串
    original_content = response['Body'].read().decode('utf-8')
    
    # 在内容开头添加 UTF-8 BOM
    utf8_with_bom_content = "\ufeff" + original_content
    
    # 将带有 BOM 的新内容写入最终文件路径
    print(f"正在将带有 BOM 的数据写入最终文件: {final_output_path}")
    s3_client.put_object(Bucket=bucket, 
                            Key=os.path.basename(final_output_path), 
                            Body=utf8_with_bom_content.encode('utf-8'))
    
    # --- 清理临时文件 ---
    print("正在清理临时文件...")
    s3_client.delete_object(Bucket=bucket, Key=part_file_key)
    # 还要清理由 Spark 创建的 _SUCCESS 文件
    success_file_key = f"{os.path.basename(temp_output_path)}/_SUCCESS"
    s3_client.delete_object(Bucket=bucket, Key=success_file_key)
    
    print("\n操作完成。")
else:
    print("未找到 part-*.csv 文件。请检查输出路径和写入操作是否成功。")
# s3_input = spark.sparkContext.textFile(part_file_path)
# s3_input.map(lambda line: "\ufeff" + line).coalesce(1).saveAsTextFile("s3a://jiaoji/output_c/")
# 写成 CSV 到 MinIO桶，指定目录
# df.write.mode("overwrite").option("header", "true").csv(output_path)

spark.stop()