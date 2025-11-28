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


FIELD_CLEANERS = {
    # 字符串类型
    "Order_ID": to_nullable_string, 
    "Order_Status": to_nullable_string,
    "Tracking_Number": to_nullable_string,
    "Logistics_Method": to_nullable_string,
    "Ship_Country": get_iso_country_code_pycountry,
    "Zip_Code": to_nullable_string,
    "Product_Name": to_nullable_string,
    "Variation_Name": to_nullable_string,
    "Shipment_Method": to_nullable_string,
    "Cancel_Reason": to_nullable_string,
    "Return_Refund_Status": to_nullable_string,
    "Remark_From_Buyer": to_nullable_string,
    "Note": to_nullable_string,
    "Recipient": to_nullable_string,
    "Phone": to_nullable_string,
    "Address_Line_1": to_nullable_string,
    "Ship_State": to_nullable_string,
    "Ship_City": to_nullable_string,
    "District": to_nullable_string,
    "Town": to_nullable_string,
    "Username_Buyer": to_nullable_string,
    "Tags": to_nullable_string,
    "Payment_Method": to_nullable_string,
    "Package_ID": to_nullable_string,
    "Store": to_nullable_string,
    "Product_ID": to_nullable_string,
    "ID": to_nullable_string,

    # Decimal / 浮点数类型
    "Estimated_Weight_g": to_decimal,
    "Original_Price": to_decimal,
    "Deal_Price": to_decimal,
    "Item_Promotion_Discount": to_decimal,
    "Credit_Card_Discount_Total": to_decimal,
    "Order_Total_Weight_g": to_decimal,
    "Store_Voucher": to_decimal,
    "Platform_Voucher": to_decimal,
    "Platform_Bundle_Discount": to_decimal,
    "Seller_Bundle_Discount": to_decimal,
    "Coin_Offset": to_decimal,
    "Buyer_Paid_Shipping_Fee": to_decimal,
    "Products_Price_Paid_By_Buyer": to_decimal,
    "Shipping_Cost": to_decimal,
    "Estimated_Shipping_Cost_CNY": to_decimal,
    "Reverse_Shipping_Fee": to_decimal,
    "Service_Fee": to_decimal,
    "SKU_Total_Weight": to_decimal,
    "Total_Amount": to_decimal,
    "Handling_Fee": to_decimal,
    "Transaction_Fee": to_decimal,
    "Commission_Fee": to_decimal,
    "Weight_kg": to_decimal,

    # 整数类型
    "Quantity": to_nullable_int,
    "Return_Quantity": to_nullable_int,
    "Number_Of_Pieces": to_nullable_int,

    # 布尔类型
    "Bundle_Deals_Indicator": to_nullable_bool,
    "Is_Best_Selling_Product": to_nullable_bool,
}

# --- SparkSession 初始化 ---
setup_logging()

spark, s3_client = get_spark_and_s3_clients(app_name="RawDataClassifier",enable_iceberg=True)


def get_file_content_as_dataframe(bucket_name, object_key: str):
    """
    从S3存储桶中读取指定对象，并将其内容解析为pandas DataFrame。
    支持 .xls/.xlsx (Excel) 和 .csv/.txt 文件。
    所有列将强制转换为字符串类型。
    Args:
        object_key (str): S3 对象的完整键（例如 'folder/subfolder/file.csv'）。
    Returns:
        pd.DataFrame or None: 如果文件成功读取并转换为 DataFrame，则返回该 DataFrame；
                              否则，如果发生错误或文件类型不受支持，则返回 None。
    """
    
    try:
        if object_key.endswith(('.xls', '.xlsx')):
            local_temp_path = f"/tmp/{os.path.basename(object_key)}"
            try:
                s3_client.download_file(bucket_name, object_key, local_temp_path)
                df = pd.read_excel(local_temp_path, dtype=str)

            except Exception as e:
                print(f"警告: 读取 XLSX 文件 '{object_key}' 失败，错误: {e}")
                return None
            finally:
                # 清理临时文件
                if os.path.exists(local_temp_path):
                    os.remove(local_temp_path)
        elif object_key.endswith('.csv'):
            obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()),encoding='utf-8', dtype=str)        
        elif object_key.endswith('.txt'):
            obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()),encoding='utf-8', sep='\t', dtype=str)
    except Exception as e:
        # 捕获函数内部更广泛的异常（如S3连接问题，或pandas.read_csv/excel的更深层错误）
        print(f"致命警告: 读取文件 '{object_key}' 过程中发生意外错误: {e}")
        return None  # 返回 None 表示读取失败

    return df

def standardize_table(original_df: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    读取df，使用不同国家的字段映射。
    
    参数：
    - original_df (pd.DataFrame): 包含原始、非标准化列名的 DataFrame。
    - country (str): 国家代码（如 "US", "JP", "CN"），用于选择对应的列映射。
    返回：
    - 标准化字段名的 DataFrame
    """
    if country not in FIELD_MAPPING_BY_COUNTRY:

        return pd.DataFrame(columns=ALLOWED_DB_COLUMNS)

    # 确保 FIELD_MAPPING_BY_COUNTRY 字典已定义且包含对应的国家映射
    mapping = FIELD_MAPPING_BY_COUNTRY[country]

    df = original_df.copy()

    # 标准化列名：strip 防止列名前后有空格
    new_columns = []
    for col in df.columns:
        # 步骤1：去除空格
        stripped_col = str(col).strip() 
        # 步骤2：应用映射，如果不存在则保留原样
        new_columns.append(mapping.get(stripped_col, stripped_col)) 
    # 步骤3：一次性更新所有列名
    df.columns = new_columns 

    standardized_df = df.reindex(columns=ALLOWED_DB_COLUMNS)
    logging.info(f"成功为国家 '{country}' 标准化列名。")

    return standardized_df

def convert_pandas_timestamp_to_python_datetime(series: pd.Series) -> pd.Series:
    # 将 pandas.Timestamp 转成 datetime.datetime（无时区 naive）
    return series.apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)

def clean_data_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    根据 FIELD_CLEANERS 和 ALLOWED_DB_COLUMNS 清洗 Pandas DataFrame。
    Args:
        df (pd.DataFrame): 待清洗的原始 DataFrame。
    Returns:
        pd.DataFrame: 清洗后的 DataFrame，只包含 ALLOWED_DB_COLUMNS 中定义的列，并应用了相应的清洗函数。
    """
    cleaned_df = pd.DataFrame()
    temp_df = df.copy() # 使用临时DataFrame以避免在循环中修改原始df

    # 循环遍历 ALLOWED_DB_COLUMNS，因为 standardize_table 已经确保它们都存在了
    for col in ALLOWED_DB_COLUMNS:
        # 如果是 ID 列，跳过，因为 ID 在最后生成
        if col == "ID" :
            continue

        # 特殊处理日期时间列
        if col in DATETIME_COLUMNS_CLEANERS:
            # 确保 Ship_Country 列在日期时间列之前被清洗过
            # 为了极端健壮性，这里可以额外检查或强制清洗，但通常不是必须的，取决于数据可靠性。
            
            # 使用 .get() 安全获取 Ship_Country 值，以防万一该列在某个特定行中为 None
            cleaned_df[col] = temp_df.apply(
                lambda row: to_datetime_aware(row[col], country_iso_code=cleaned_df['Ship_Country'][row.name]),
                axis=1
            )
        else:
            # 对于所有其他列 (包括 Ship_Country)，使用 FIELD_CLEANERS 中的清洗函数
            cleaner = FIELD_CLEANERS.get(col)
            if cleaner:
                cleaned_df[col] = temp_df[col].apply(cleaner)
            else:
                # 如果没有找到清洗函数，保留原始值
                # 注意：如果 standardize_table 添加的列是 NaN/None，这里会保留 NaN/None
                # Variation_Name 的默认值处理可以在这里做
                if col == "Variation_Name":
                    cleaned_df[col] = temp_df[col].fillna("N/A_Variant")
                elif col == "Currency":
                    cleaned_df[col] = temp_df.apply(
                    lambda row: clean_currency(row[col], country_iso_code=cleaned_df['Ship_Country'][row.name]),
                    axis=1
                )              
                elif col == "Region":
                  
                    cleaned_df[col] = temp_df.apply(
                    lambda row: clean_region(row[col], country_iso_code=cleaned_df['Ship_Country'][row.name]),
                    axis=1
                )
                #     currency=CURRENCIES[temp_df[col]]
                #     print(currency)
                #     cleaned_df[col] = temp_df[col].fillna(currency)

                else:
                    cleaned_df[col] = temp_df[col]

    # 统一处理 NaN, NaT 为 None，以便后续写入数据库
    for col in cleaned_df.columns:
        if cleaned_df[col].dtype == 'datetime64[ns]':
            cleaned_df[col] = cleaned_df[col].replace({pd.NaT: None})
            cleaned_df[col] = convert_pandas_timestamp_to_python_datetime(cleaned_df[col])
        else:
            # 对于所有其他列，将 np.nan 转换为 None
            cleaned_df[col] = cleaned_df[col].replace({np.nan: None})
            
    cleaned_df = cleaned_df.replace({pd.NaT: None, np.nan: None})

     # 为每一行生成一个 UUID 作为 Order_Item_Line_ID
    cleaned_df['ID'] = [uuid.uuid4().hex for _ in range(len(cleaned_df))]
    
    standardized_df = cleaned_df.reindex(columns=ALLOWED_DB_COLUMNS)

    return standardized_df

# --- 主分类流程 ---
print(f"开始分类原始数据，从 's3a://{RAW_DATA_BUCKET}/{CLASSIFIED_PREFIX}' 读取...")

try:
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=RAW_DATA_BUCKET, Prefix=CLASSIFIED_PREFIX)

    files_to_process = []
    for page in pages:
        for obj in page.get('Contents', []):
            if not obj['Key'].endswith('/'): 
                files_to_process.append(obj['Key'])

except Exception as e:
    print(f"错误: 列出 '{CLASSIFIED_PREFIX}' 中的文件失败: {e}")
    files_to_process = []

if not files_to_process:
    print(f"'{CLASSIFIED_PREFIX}' 中没有新的文件需要分类。")
else:
    print(f"找到 {len(files_to_process)} 个文件需要分类。")
    
    for object_key in files_to_process:
        print(f"\n正在处理文件: {object_key}")
        file_name = os.path.basename(object_key)

        # 1. 从文件路径中提取 Shop ID
        country_path = os.path.dirname(object_key)
        shop_path = os.path.dirname(country_path)
        country_id = os.path.basename(country_path)
        shop_id = os.path.basename(shop_path)

        original_df= get_file_content_as_dataframe(RAW_DATA_BUCKET, object_key)
        df = standardize_table(original_df, country_id)
        cleaned_df=clean_data_frame(df)
       
        cleaned_df['Store'] = shop_id

    # 完整定义你的 Spark Schema
    # 务必根据你的数据预期类型和清洗结果来定义
        _original_field_definitions_map = {
    "ID": StructField("ID", StringType(), True),
    "Order_ID": StructField("Order_ID", StringType(), True),
    "Currency": StructField("Currency", StringType(), True),
    "Region": StructField("Region", StringType(), True),
    "Order_Status": StructField("Order_Status", StringType(), True),
    "Purchase_Datetime": StructField("Purchase_Datetime", TimestampType(), True),
    "Store": StructField("Store", StringType(), True),
    "Payment_Datetime": StructField("Payment_Datetime", TimestampType(), True),
    "Tracking_Number": StructField("Tracking_Number", StringType(), True),
    "Logistics_Method": StructField("Logistics_Method", StringType(), True),
    "Ship_Datetime": StructField("Ship_Datetime", TimestampType(), True),
    "Estimated_Ship_Out_Datetime": StructField("Estimated_Ship_Out_Datetime", TimestampType(), True),
    "Ship_Country": StructField("Ship_Country", StringType(), True),
    "Estimated_Weight_g": StructField("Estimated_Weight_g", DecimalType(10, 2), True),
    "Zip_Code": StructField("Zip_Code", StringType(), True),
    "Product_Name": StructField("Product_Name", StringType(), True),
    "Variation_Name": StructField("Variation_Name", StringType(), True),
    "Shipment_Method": StructField("Shipment_Method", StringType(), True),
    "Cancel_Reason": StructField("Cancel_Reason", StringType(), True),
    "Return_Refund_Status": StructField("Return_Refund_Status", StringType(), True),
    "Original_Price": StructField("Original_Price", DecimalType(10, 2), True),
    "Product_ID": StructField("Product_ID", StringType(), True),
    "Deal_Price": StructField("Deal_Price", DecimalType(10, 2), True),
    "Quantity": StructField("Quantity", IntegerType(), True),
    "Return_Quantity": StructField("Return_Quantity", IntegerType(), True), # Note: This was just 'IntegerType()' in original, should be StructField. Correcting below.
    "Item_Promotion_Discount": StructField("Item_Promotion_Discount", DecimalType(10, 2), True),
    "Credit_Card_Discount_Total": StructField("Credit_Card_Discount_Total", DecimalType(10, 2), True),
    "Remark_From_Buyer": StructField("Remark_From_Buyer", StringType(), True),
    "Bundle_Deals_Indicator": StructField("Bundle_Deals_Indicator", BooleanType(), True),
    "Note": StructField("Note", StringType(), True),
    "Recipient": StructField("Recipient", StringType(), True),
    "Phone": StructField("Phone", StringType(), True),
    "Address_Line_1": StructField("Address_Line_1", StringType(), True),
    "Ship_State": StructField("Ship_State", StringType(), True),
    "Ship_City": StructField("Ship_City", StringType(), True), # Corrected: ensure it's a proper StructField
    "District": StructField("District", StringType(), True),
    "Town": StructField("Town", StringType(), True),
    "Username_Buyer": StructField("Username_Buyer", StringType(), True),
    "Order_Complete_Datetime": StructField("Order_Complete_Datetime", TimestampType(), True),
    "Number_Of_Pieces": StructField("Number_Of_Pieces", IntegerType(), True),
    "Order_Total_Weight_g": StructField("Order_Total_Weight_g", DecimalType(10, 2), True),
    "Store_Voucher": StructField("Store_Voucher", DecimalType(10, 2), True),
    "Platform_Voucher": StructField("Platform_Voucher", DecimalType(10, 2), True),
    "Platform_Bundle_Discount": StructField("Platform_Bundle_Discount", DecimalType(10, 2), True),
    "Seller_Bundle_Discount": StructField("Seller_Bundle_Discount", DecimalType(10, 2), True),
    "Coin_Offset": StructField("Coin_Offset", DecimalType(10, 2), True),
    "Buyer_Paid_Shipping_Fee": StructField("Buyer_Paid_Shipping_Fee", DecimalType(10, 2), True),
    "Products_Price_Paid_By_Buyer": StructField("Products_Price_Paid_By_Buyer", DecimalType(10, 2), True),
    "Shipping_Cost": StructField("Shipping_Cost", DecimalType(10, 2), True),
    "Estimated_Shipping_Cost_CNY": StructField("Estimated_Shipping_Cost_CNY", DecimalType(10, 2), True),
    "Reverse_Shipping_Fee": StructField("Reverse_Shipping_Fee", DecimalType(10, 2), True),
    "Service_Fee": StructField("Service_Fee", DecimalType(10, 2), True),
    "SKU_Total_Weight": StructField("SKU_Total_Weight", DecimalType(10, 2), True),
    "Total_Amount": StructField("Total_Amount", DecimalType(10, 2), True),
    "Tags": StructField("Tags", StringType(), True),
    "Order_Cancel_Datetime": StructField("Order_Cancel_Datetime", TimestampType(), True),
    "Handling_Fee": StructField("Handling_Fee", DecimalType(10, 2), True),
    "Payment_Method": StructField("Payment_Method", StringType(), True),
    "Transaction_Fee": StructField("Transaction_Fee", DecimalType(10, 2), True),
    "Commission_Fee": StructField("Commission_Fee", DecimalType(10, 2), True),
    "Package_ID": StructField("Package_ID", StringType(), True),
    "Is_Best_Selling_Product": StructField("Is_Best_Selling_Product", BooleanType(), True),
    "Weight_kg": StructField("Weight_kg", DecimalType(10, 2), True)

}
        for c in DATETIME_COLUMNS_CLEANERS:
            if c in cleaned_df.columns:
                cleaned_df[c] = cleaned_df[c].astype(str)


        spark_schema = StructType([])

        for col_name in ALLOWED_DB_COLUMNS:
            if col_name in DATETIME_COLUMNS_CLEANERS:
                spark_schema.add(StructField(col_name, StringType(), True))
            elif col_name in _original_field_definitions_map:
                field = _original_field_definitions_map[col_name]
                if isinstance(field.dataType, TimestampType):
                    spark_schema.add(StructField(col_name, StringType(), True))  # 关键点
                else:
                    spark_schema.add(field)
            else:
                spark_schema.add(StructField(col_name, StringType(), True))
                        # 💡 注意顺序：过滤列后再 createDataFrame
        cleaned_df = cleaned_df[ALLOWED_DB_COLUMNS]

        # ✅ 传 dict 列表创建 DataFrame（传 DataFrame 本身也可以）
        spark_df = spark.createDataFrame(cleaned_df.to_dict(orient="records"), schema=spark_schema)

        for c in DATETIME_COLUMNS_CLEANERS:
            if c in spark_df.columns:
                spark_df = spark_df.withColumn(c, to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss"))

       
        table_name = "iceberg_mysql_catalog.total_orders_db.total_orders"
        target_schema = spark.table(table_name).schema

        for field in target_schema:
            if field.name not in spark_df.columns:
                spark_df = spark_df.withColumn(field.name, lit(None).cast(field.dataType))

        # 按照表字段顺序排序
        spark_df = spark_df.select([field.name for field in target_schema])

        # 写入
        spark_df.writeTo(table_name).using("iceberg").append()



        # target_object_key = f"{CLASSIFIED_PREFIX}{shop_id}/{country_code}/{file_name}"
        # move_s3_object(RAW_DATA_BUCKET, object_key, RAW_DATA_BUCKET, target_object_key)






