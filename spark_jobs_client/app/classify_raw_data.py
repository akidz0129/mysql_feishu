import openpyxl
import os
from common_utils import get_spark_and_s3_clients
from _config import RAW_DATA_BUCKET, UNCLASSIFIED_ROOT_PREFIX, CLASSIFIED_PREFIX, UNIDENTIFIED_RAW_PREFIX
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max
from pyspark.sql.types import TimestampType
from dateutil import parser
from datetime import datetime, date
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_timestamp,coalesce

# --- SparkSession 初始化 ---
spark, s3_client = get_spark_and_s3_clients(app_name="RawDataClassifier")

# --- MinIO 桶和路径定义 ---


# --- 文件识别规则定义 ---
# --- 根据店铺名直接判断国家的规则 ---
# 字典格式：{ "店铺名": "国家代码" }
# 例如：某些店铺的订单只来自一个国家
SHOP_TO_COUNTRY_MAP = {
    "宇翔": "amazon",   
    "惠美美国": "amazon",   
    "lazada主店": "lazada",   
    "tiktok跨境店": "tiktok"
}
# 规则可以是：(关键词列表, shop_id, country_code)
# 匹配顺序很重要，更具体的规则放在前面

CLASSIFICATION_RULES = [
    # (["amazon-order-id","merchant-order-id"], "amazon"),
    # 菲律宾订单
    (["Order ID","Order Status","Estimated Order Weight","Price Discount(from Seller)(PHP)"], "feilv"),

    # 泰国 订单
    (["หมายเลขคำสั่งซื้อ","สถานะการสั่งซื้อ"], "taiguo"),

    # 越南订单
    (["Mã đơn hàng","Trạng Thái Đơn Hàng"], "yuenan"),

    # 马来西亚订单
    (["Order ID","Order Status","Estimated Order Weight","Credit Card Discount Total"], "malai"),

    # 印尼订单
    (["Status Pembatalan/ Pengembalian","Harga Awal","Nomor Referensi SKU"], "yinni"),
    # (["Order ID","Order Status","Created Time"], "tiktok"),
    # (["orderItemId","status","createTime"], "lazada"),
    # 兜底规则，如果以上规则都不匹配，则标记为无法识别的国家
    ([], "unknown_country") # 放在最后作为默认，应始终匹配
]

COUNTRY_COLUMN_MAP = {
    "feilv": "Order Creation Date",
    "taiguo": "วันที่ทำการสั่งซื้อ",
    "yuenan": "Ngày đặt hàng",
    "malai": "Order Creation Date",
    "yinni": "Waktu Pesanan Dibuat",
    "tiktok": "Created Time",
    "lazada": "createTime"
}

def extract_date_range_from_file(bucket_name,file_path:str, country_code):
    """
    使用 Spark 读取 Excel (spark-excel) 或 CSV 文件，
    根据国家代码获取对应时间列，提取最早和最晚订购时间（格式 yyMMdd-yyMMdd）。
    """
    if country_code not in COUNTRY_COLUMN_MAP:
        print(f"  - ❌ 未知国家代码: {country_code}")
        return None

    time_col = COUNTRY_COLUMN_MAP[country_code]
    print(f"DEBUG: - 识别到的时间列名 (来自 COUNTRY_COLUMN_MAP): {time_col}")


    formats = [
        'yyyy-MM-dd\'T\'HH:mm:ssXXX',      # 带时区，最优先
        'yyyy-MM-dd\'T\'HH:mm:ss',        # ISO 格式
        'dd MMM yyy HH:mm',           # 11 Jul 2025 11:14
        'dd/MM/yyyy HH:mm:ss',        # 07/07/2025 12:03:14
        'yyyy-MM-dd HH:mm',           # 2025-06-09 23:59
        'yyyy-MM-dd',                 # 只有日期
    ]
    complete_file_path=os.path.join("s3a://",bucket_name,file_path)

    if complete_file_path.endswith(('.xls', '.xlsx')):
        df = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(complete_file_path)

    elif complete_file_path.endswith(('.csv','.txt')):
        df = spark.read.option("header", True).csv(complete_file_path)
    else:
        print("  - ❌ 不支持的文件格式: ")
        return None
    try:
        timestamp_expressions = [to_timestamp(col(time_col), fmt) for fmt in formats]
                # 使用 coalesce 找到第一个非 NULL 的时间戳
        df2 = df.withColumn("parsed_time", coalesce(*timestamp_expressions))
        
        # df2 = df.withColumn("parsed_time", col(time_col).cast(TimestampType()))
        df_filtered = df2.filter((col("parsed_time").isNotNull()) & (col("parsed_time") <= current_timestamp()))

        # 取最小最大时间
        row = df_filtered.select(spark_min("parsed_time").alias("min_time"), spark_max("parsed_time").alias("max_time")).collect()

        if not row or row[0]["min_time"] is None or row[0]["max_time"] is None:
            print("  - ⚠️ 无有效订购时间数据")
            return None

        min_time = row[0]["min_time"].strftime("%y%m%d")
        max_time = row[0]["max_time"].strftime("%y%m%d")
        return f"{min_time}_{max_time}"
    except Exception as e:
        print(f"  - ❌ 读取或解析异常: {e}")
        return None

def identify_country_from_content(bucket_name, object_key):
    """
    根据文件内容（读取前几行）来识别国家代码。支持 CSV 和 XLSX 文件。
    """

    file_content_sample = "" # 用于存储提取出的文本内容样本
    try:
        if object_key.endswith(('.xls', '.xlsx')):
            print(f"   - 检测到 XLSX 文件。正在下载并读取内容...")
            # 对于 XLSX 文件，需要将整个文件下载到临时位置，然后用 openpyxl 读取
            # 注意：这会消耗更多内存和时间，特别是对于大型文件。
            # Spark 原生处理 Excel 通常会使用 spark-excel 等连接器，但这里是纯 Python 识别，所以需要下载。
            local_temp_path = f"/tmp/{os.path.basename(object_key)}"
            
            try:
                s3_client.download_file(bucket_name, object_key, local_temp_path)
                workbook = openpyxl.load_workbook(local_temp_path)
                sheet = workbook.active # 默认读取第一个活跃的 sheet
                
                # 读取前几行作为样本，例如前5行，并将单元格内容合并为字符串
                row_values = []
                # 直接获取第一行（index 1）
                row_cells = sheet[1] 
                for cell in row_cells:
                    if cell.value is not None:
                        row_values.append(str(cell.value))
                file_content_sample += " ".join(row_values) + "\n" # 用空格连接单元格，换行分隔行
            
                workbook.close()
            except Exception as e:
                print(f"警告: 读取 XLSX 文件 '{object_key}' 失败，错误: {e}")
                return "error_country" # 读取 XLSX 失败，标记为错误国家
            finally:
                # 清理临时文件
                if os.path.exists(local_temp_path):
                    os.remove(local_temp_path)

        elif object_key.endswith(('.csv','.txt')):
            print(f"   - 检测到 CSV 文件。正在读取内容样本...")
            # 对于 CSV 文件，读取文件的前几行（例如前10KB）作为样本
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key, Range='bytes=0-10240')
            file_content_sample = response['Body'].read().decode('utf-8', errors='ignore')

        else:
            print(f"   - 警告: 文件 '{object_key}' 是未知或不支持的类型。跳过内容识别。")
            return "unknown_file_type" # 新增一个返回类型，表示文件类型无法识别
        # 遍历分类规则进行匹配

        for keywords, country_code in CLASSIFICATION_RULES:
            # 检查所有关键词是否都在文件内容样本中 (严格区分大小写)
            all_keywords_present = True
            for keyword in keywords:
                # 直接使用 keyword 和 file_content_sample 进行比较，不进行大小写转换
                if keyword not in file_content_sample:
                    all_keywords_present = False
                    # 打印调试信息时，关键词也不转换为小写
                    print(f"   DEBUG: 关键词 '{keyword}' 在规则 '{country_code}' 中未找到。")
                    break
            if all_keywords_present:
                print(f"   - 识别到国家: {country_code}")
                return country_code
        
            
        # 如果所有规则都未匹配到（通常是兜底规则匹配到，但其关键词列表为空）
        print(f"  - 未匹配到任何具体国家规则。默认归类为 'unknown_country'。")
        return "unknown_country" 
    except Exception as e:
        print(f"错误: 识别文件国家属性失败，文件: {object_key}, 错误: {e}")
        return "error_country" # 识别过程中发生错误

def extract_shop_id_from_path(object_key, root_prefix):
    """
    从 S3 对象的 key 中提取 shop_id。
    假设 shop_id 是直接包含文件的那个文件夹的名称。
    Args:
        object_key (str): S3 对象的完整 key (路径)。
        root_prefix (str): 文件所在的根前缀，用于初步验证路径结构。
                          例如: 'incoming_unclassified/'
    Returns:
        str: 提取到的 shop_id，如果无法提取或路径不符合预期则返回 "unknown_shop"。
    """
    if not object_key.startswith(root_prefix):
        print(f"警告: 文件路径 {object_key} 不以根前缀 {root_prefix} 开头。")
        return "unknown_shop"
    
    dir_path = os.path.dirname(object_key)
    
    shop_id = os.path.basename(dir_path)
    # 步骤4: 验证提取到的 shop_id 是否有效
    if shop_id: # 检查是否是空字符串 (例如，如果文件直接位于根目录，dir_path 可能是空)
        return shop_id
    
    print(f"警告: 无法从路径 {object_key} 中提取 Shop ID。")
    return "unknown_shop"

def move_s3_object(source_bucket, source_key, target_bucket, target_key):
    """
    将S3对象从源路径移动到目标路径 (通过 copy 然后 delete 实现)。
    """
    try:
        s3_client.copy_object(
            Bucket=target_bucket,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Key=target_key
        )
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        print(f"  - 成功移动文件: {source_key} 到 {target_key}")
        return True
    except Exception as e:
        print(f"  - 移动文件失败: {source_key} 到 {target_key}. 错误: {e}")
        return False

# --- 主分类流程 ---
print(f"开始分类原始数据，从 's3a://{RAW_DATA_BUCKET}/{UNCLASSIFIED_ROOT_PREFIX}' 读取...")

try:
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=RAW_DATA_BUCKET, Prefix=UNCLASSIFIED_ROOT_PREFIX)

    files_to_process = []
    for page in pages:
        for obj in page.get('Contents', []):
            if not obj['Key'].endswith('/'): 
                files_to_process.append(obj['Key'])

except Exception as e:
    print(f"错误: 列出 '{UNCLASSIFIED_ROOT_PREFIX}' 中的文件失败: {e}")
    files_to_process = []

if not files_to_process:
    print(f"'{UNCLASSIFIED_ROOT_PREFIX}' 中没有新的文件需要分类。")
else:
    print(f"找到 {len(files_to_process)} 个文件需要分类。")
    
    for object_key in files_to_process:
        print(f"\n正在处理文件: {object_key}")
        file_name = os.path.basename(object_key)

        # 1. 从文件路径中提取 Shop ID
        dir_path = os.path.dirname(object_key)
        shop_id = os.path.basename(dir_path)
        
        country_code = "unknown_country" # 初始化为未知国家

        # 2. 尝试通过 SHOP_TO_COUNTRY_MAP 直接判断国家
        if shop_id in SHOP_TO_COUNTRY_MAP:
            country_code = SHOP_TO_COUNTRY_MAP[shop_id]
            print(f"  - (路径识别) 通过店铺名 '{shop_id}' 直接判断国家为: {country_code}")
        else:
            # 3. 如果 ShopID 不在映射中，则通过文件内容识别国家
            print(f"  - (内容识别) 店铺 '{shop_id}' 不在直判规则中，将尝试通过文件内容识别国家...")
            country_code = identify_country_from_content(RAW_DATA_BUCKET, object_key)

        report_date_obj = datetime.now()
        import re
        file_match=re.match(r"^.*\.(.*)",object_key)
        file_suffix=file_match.group(1)

        date_range = extract_date_range_from_file(RAW_DATA_BUCKET,object_key, country_code)
        print("付款时间范围:", date_range)

        if date_range:
            target_file_name = f"{country_code}_{date_range}.{file_suffix}"

        print(target_file_name)
        # # 根据识别结果决定移动到哪里
        # # 只有当 shop_id 和 country_code 都有效时才移动到分类区
        # if shop_id != "unknown_shop" and country_code not in ["error_country", "unknown_country"]:
        #     target_object_key = f"{CLASSIFIED_PREFIX}{shop_id}/{country_code}/{target_file_name}"
        #     move_s3_object(RAW_DATA_BUCKET, object_key, RAW_DATA_BUCKET, target_object_key)
        # else:
        #     target_unidentified_key = f"{UNIDENTIFIED_RAW_PREFIX}{shop_id}/{file_name}"
        #     print(f"  - 无法完全识别文件 {file_name} (Shop: {shop_id}, Country: {country_code})。移动到 '{UNIDENTIFIED_RAW_PREFIX}'。")
        #     move_s3_object(RAW_DATA_BUCKET, object_key, RAW_DATA_BUCKET, target_unidentified_key)

print("\n原始数据分类过程完成。")