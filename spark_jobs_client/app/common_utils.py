import logging
import os
from pyspark.sql import SparkSession
import boto3
import pandas as pd
from datetime import datetime
from decimal import Decimal, InvalidOperation
import pycountry
from _config import COUNTRY_INFO
import socket
import time
import pytz

def setup_logging():
    """
    配置应用程序的日志系统。
    这个函数应该在 Spark 任务启动时被调用一次。
    """
    # 获取根 logger。通常，我们配置根 logger，这样所有子 logger 都会继承其设置。
    root_logger = logging.getLogger()

    # 避免重复添加处理器，这在某些情况下（如在 Jupyter/Databricks Notebook 中多次运行）很重要
    # Spark 任务通常只运行一次，但这是一个好的实践
    if not root_logger.handlers:
        # 创建一个 StreamHandler，将日志输出到控制台（Spark 控制台或 YARN 日志）
        handler = logging.StreamHandler()

        # 设置日志格式。'%name' 会显示 logger 的名称（例如 'pyspark', 'your_module_name'）
        # 这是非常推荐的，因为在 Spark 日志中区分来源很重要
        formatter = logging.Formatter('%(asctime)s %(name)-15s %(levelname)-8s %(message)s [%(filename)s:%(lineno)d]')
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        
        # 从环境变量获取日志级别，如果没有则默认为 INFO
        log_level_str = os.environ.get('LOG_LEVEL', 'INFO').upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        root_logger.setLevel(log_level)

        # PySpark 默认会有一个 INFO 级别的 ConsoleHandler。
        # 通常，你的自定义 Handler 会优先处理。

        root_logger.info("日志系统已初始化。")

def get_minio_ip():
        minio_host = "minio"
        minio_ip = None
        logging.info("正在尝试解析 MinIO IP 地址...")
        try:
            # 尝试解析MinIO的主机名到IP地址
            minio_ip = socket.gethostbyname(minio_host)
            logging.info(f"已解析 MinIO IP: {minio_ip}")
        except socket.gaierror as e:
            logging.warning(f"解析 MinIO 主机名 '{minio_host}' 失败: {e}. 等待并重试...")
            # 如果解析失败，可能是MinIO服务还没完全启动或网络未就绪，等待并重试
            for i in range(1, 11): # 尝试10次
                time.sleep(2)
                try:
                    minio_ip = socket.gethostbyname(minio_host)
                    logging.info(f"第 {i} 次重试成功，已解析 MinIO IP: {minio_ip}")
                    break
                except socket.gaierror:
                    logging.warning(f"第 {i} 次重试失败，继续等待...")
            if not minio_ip:
                logging.error(f"多次重试后仍无法解析 MinIO 主机 '{minio_host}'。任务失败。")
                raise Exception(f"无法解析 MinIO 主机 '{minio_host}'。")

        minio_url=f"http://{minio_ip}:9000"
        return minio_url
        
def get_spark_and_s3_clients(app_name="DefaultSparkApplication",enable_iceberg=False):
    """
    初始化并返回 SparkSession 和 boto3 S3 客户端。
    Spark 配置从 spark-defaults.conf 加载，敏感凭据从环境变量获取。
    """
    # --- SparkSession 初始化 ---
    # 从环境变量中获取 MySQL JDBC 凭据
    mysql_user = os.environ.get("MYSQL_ICEBERG_CATALOG_USER")
    mysql_password = os.environ.get("MYSQL_ICEBERG_CATALOG_PASSWORD")
    mysql_db_name = os.environ.get("MYSQL_ICEBERG_CATALOG_DB")
    
    builder=SparkSession.builder.appName(app_name) 

    if enable_iceberg:
        minio_url=get_minio_ip()
        builder = builder\
        .config("spark.hadoop.fs.s3a.endpoint", minio_url) \
        .config("spark.sql.catalog.iceberg_mysql_catalog.s3.endpoint", minio_url) \
        .config("spark.sql.catalog.iceberg_mysql_catalog.jdbc.user", mysql_user) \
        .config("spark.sql.catalog.iceberg_mysql_catalog.jdbc.password", mysql_password) \
        .config("spark.sql.catalog.iceberg_mysql_catalog.uri", f"jdbc:mysql://mysql-db:3306/{mysql_db_name}?useSSL=false&allowPublicKeyRetrieval=true")\
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "iceberg_mysql_catalog") \
        .config("spark.sql.catalog.iceberg_mysql_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_mysql_catalog.type", "jdbc") \
        .config("spark.sql.catalog.iceberg_mysql_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg_mysql_catalog.s3.region", "us-east-1") \
        .config("spark.sql.catalog.iceberg_mysql_catalog.delegate", "spark_catalog") \
        .config("spark.sql.catalog.iceberg_mysql_catalog.warehouse", "s3a://iceberg-warehouse/") \
        # .config("spark.sql.catalog.iceberg_mysql_catalog.s3.access-key", minio_access_key) \
        # .config("spark.sql.catalog.iceberg_mysql_catalog.s3.secret-key", minio_secret_key) \
        # .config("spark.sql.catalog.iceberg_mysql_catalog.jdbc.schema-version", "V1")
    
        # 所以这里不再需要重复配置除了凭据和动态数据库名之外的 catalog 属性

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # --- MinIO S3 客户端初始化 ---
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )

    return spark, s3_client

# 如果你需要单独获取 SparkSession 或 s3_client
def get_spark_session(app_name="DefaultSparkApplication"):
    spark, _ = get_spark_and_s3_clients(app_name)
    return spark

def get_s3_client():
    _, s3_client = get_spark_and_s3_clients() # 这里会再次初始化SparkSession，但getOrCreate会返回现有
    return s3_client


def to_decimal(value):
    # 修改：在尝试 Decimal 转换前，更彻底地清理字符串，
    # 移除千位分隔符和除了数字、小数点、负号以外的所有字符。
    if pd.isna(value) or str(value).strip() == '':
        return Decimal('0.00')
    try:
        # 将值转换为字符串，移除逗号，然后只保留数字、小数点和负号
        clean_val = str(value).strip()
        # 移除货币符号、千位分隔符等，只保留数字和 '.' '-'
        # 注意：这个正则表达式假设数字格式是标准的，没有其他复杂字符
        clean_val = ''.join(c for c in clean_val if c.isdigit() or c == '.' or c == '-')
        if not clean_val: # 如果清理后变成空字符串，也视为空值
            return Decimal('0.00')
        return Decimal(clean_val)
    except InvalidOperation:
        # 使用 logging 替代 print，方便统一日志管理
        logging.warning(f"警告: 无法将 '{value}' 转换为decimal. 返回 0.00。")
        return Decimal('0.00')
    
def to_nullable_string(value):
    # 这样 MySQL 的 VARCHAR/CHAR 字段可以存储 NULL
    if pd.isna(value) or str(value).strip() == '':
        return None
    return str(value).strip() # 确保是字符串

def to_nullable_int(value):
    # 修改：尝试使用 pd.to_numeric 更稳健地处理，并最终转换为 Python int 或 0
    if pd.isna(value) or value is None or str(value).strip() == '':
        return 0
    try:
        # 使用 pd.to_numeric 可以处理更多格式的数字字符串，errors='coerce' 将无效值转为 NaN
        numeric_val = pd.to_numeric(str(value).strip(), errors='coerce')
        if pd.isna(numeric_val): # 如果转换为 NaN，说明无法转为数字
            logging.warning(f"警告: 无法将 '{value}' 转换为整数. 返回 0。")
            return 0
        return int(numeric_val) # 转换为整数
    except ValueError: # 实际上 pd.to_numeric 很少会抛出 ValueError，更多是 InvalidOperation 或其他
        logging.warning(f"警告: 无法将 '{value}' 转换为整数. 返回 0。")
        return 0

def to_nullable_bool(value):
    # 处理 NaN, None 或空字符串，统一默认返回 False
    if pd.isna(value) or str(value).strip() == '':
        return False
    
    val_str = str(value).strip().lower()
    
    if val_str in ('true', 'y', '1'):
        return True
    # Map 'false', 'n', '0' to False
    elif val_str in ('false', 'n', '0'):
        return False
    else:
        print(f"警告: 无法识别的布尔值: '{value}'. 默认为 False.")
        return False
    
def to_datetime(value):
    """
    无效值返回 None。
    """
    if pd.isna(value) or str(value).strip() == '' or str(value).strip() == '-':
        return None
    formats = [
        '%d %b %Y %H:%M',           # 11 Jul 2025 11:14
        '%d/%m/%Y %H:%M:%S',        # 07/07/2025 12:03:14 (日/月/年)
        '%Y-%m-%d %H:%M',           # 2025-06-09 23:59
        '%Y-%m-%dT%H:%M:%S%z',      # 2025-07-03T05:26:51+00:00 (带时区)
    ]
    val_str = str(value).strip()
    for fmt in formats:
        try:
            return datetime.strptime(val_str, fmt)
        except ValueError:
            continue
    print(f"警告: 无法将 '{value}' 转换为日期时间格式。返回 None。")
    return None

def clean_currency(value, country_iso_code=None):
    if pd.notna(value):
        return value
    country = COUNTRY_INFO[country_iso_code]
    return country["Currency"]

def clean_region(value, country_iso_code=None):
    if pd.notna(value):
        return value
    country = COUNTRY_INFO[country_iso_code]
    return country["Region"]

# 3. 默认时区（当国家无法识别或没有特定时区映射时使用）
DEFAULT_TIMEZONE = 'UTC'
def to_datetime_aware(value, country_iso_code=None):
    """
    将各种日期时间字符串转换为带时区信息的 datetime 对象，并统一转换为 UTC。
    对于不带时区信息的字符串，根据提供的国家 ISO 代码确定其原始时区。
    无效值或无法解析的值返回 None。

    Args:
        value (str): 要转换的日期时间字符串。
        country_iso_code (str, optional): 数据的来源国家 ISO 3166-1 alpha-2 代码
                                         (例如 'PH', 'TH', 'SG' 等)。
                                         如果为 None，且时间字符串不含时区，则默认为 UTC。

    Returns:时间戳
    """
    if pd.isna(value) or str(value).strip() == '' or str(value).strip() == '-':
        return None

    # 定义所有可能的时间格式，包括带时区和不带时区的
    formats = [
        '%Y-%m-%dT%H:%M:%S%z',      # 2025-07-03T05:26:51+00:00 (带时区，优先匹配)
        '%Y-%m-%dT%H:%M:%S',        # 2025-07-03T05:26:51 (ISO格式，无时区)
        '%d %b %Y %H:%M',           # 11 Jul 2025 11:14 (无时区)
        '%d/%m/%Y %H:%M:%S',        # 07/07/2025 12:03:14 (日/月/年, 无时区)
        '%Y-%m-%d %H:%M',           # 2025-06-09 23:59 (无时区)
        '%Y-%m-%d',                 # 只含日期，默认时间0点
    ]
    val_str = str(value).strip()

    dt_obj = None
    for fmt in formats:
        try:
            dt_obj = datetime.strptime(val_str, fmt)
            break # 成功解析后退出循环
        except ValueError:
            continue

    if dt_obj is None:
        logging.warning(f"警告: 无法将 '{value}' 转换为日期时间格式。返回 None。")
        return None

     # 如果已经带时区信息，转换为 UTC 并去掉时区返回 naive datetime
    if dt_obj.tzinfo is not None:
        dt_utc = dt_obj.astimezone(pytz.UTC)
        return dt_utc.replace(tzinfo=None)

    # 没有时区信息，按国家时区处理，转换为 UTC 再去掉时区返回 naive datetime
    tz_name = None
    if country_iso_code and country_iso_code in COUNTRY_INFO:
        tz_name = COUNTRY_INFO[country_iso_code]["Timezone"]
    else:
        tz_name = DEFAULT_TIMEZONE  # 默认时区，如 UTC

    try:
        local_tz = pytz.timezone(tz_name)
        aware_dt = local_tz.localize(dt_obj, is_dst=None)
        aware_dt_utc = aware_dt.astimezone(pytz.UTC)
        return aware_dt_utc.replace(tzinfo=None)
    except pytz.UnknownTimeZoneError:
        logging.warning(f"警告: 未知的时区 '{tz_name}'。将天真时间视为 UTC。")
        return dt_obj.replace(tzinfo=None)  # 这里去掉时区，视为 UTC
    except Exception as e:
        logging.warning(f"警告: 处理日期时间 '{value}' 及国家 '{country_iso_code}' 时发生错误: {e}。返回 None。")
        return None



NON_LATIN_COUNTRY_MAP = {
    'เวียดนาม': 'Vietnam', # 泰语的“越南”
    '日本': 'Japan', # 日语的“日本”
    '韓国': 'Korea, Republic of', # 日语的“韩国”
    '中国': 'China', # 日语的“中国” 或 汉语
    '대한민국': 'Korea, Republic of', # 韩语的“大韩民国”
    'ประเทศไทย': 'Thailand', # 泰语的“泰国”
    'สหรัฐอเมริกา': 'United States', # 泰语的“美国”
      # --- 以下是根据你的警告信息新增的映射 ---
    'ไทย': 'Thailand',          # 泰语的“泰国”
    'ราชอาณาจักรไทย': 'Thailand', # 泰语的“泰王国”
    'ထိုင်း': 'Thailand',        # 缅甸语的“泰国”
    # --- 以下是根据你最新警告信息新增的映射 ---
    'မလေးရှား': 'Malaysia',     # 缅甸语的“马来西亚”
    '马来西亚': 'Malaysia',     # 中文的“马来西亚”
    'Malesia': 'Malaysia',       # “马来西亚”的变体/拼写错误
    # ... 根据你的数据需要，继续添加其他未成功映射的名称
}

def get_iso_country_code_pycountry(country_name_raw, default_code='XX'):
    """
    尝试将原始国家名称转换为 ISO 3166-1 alpha-2 代码。
    支持非拉丁字符的预映射。
    如果无法映射，返回 default_code。
    """
    if pd.isna(country_name_raw) or str(country_name_raw).strip() == '':
        
        return None # 或 default_code，取决于你的业务需求
    
    normalized_name = str(country_name_raw).strip()

    # --- Step 1: 尝试通过自定义非拉丁映射进行转换 ---
    # 先尝试精确匹配 NON_LATIN_COUNTRY_MAP 的键
    if normalized_name in NON_LATIN_COUNTRY_MAP:
        normalized_name = NON_LATIN_COUNTRY_MAP[normalized_name]
        # print(f"已通过自定义映射将 '{country_name_raw}' 转换为 '{normalized_name}'。")

    # --- Step 2: 使用 pycountry 进行查找 ---
    # pycountry 查找通常对大小写和一些常见变体有内置处理，
    # 但为了更稳健，可以转换为首字母大写或小写再尝试。
    
    
    country = None

    # 尝试通过 common_name 查找（通常是常用的英文名）
    try:
        country = pycountry.countries.get(common_name=normalized_name)
    except KeyError:
        pass
    
    # 如果没找到，尝试通过 name 查找
    if not country:
        try:
            country = pycountry.countries.get(name=normalized_name)
        except KeyError:
            pass
            
    # 如果还没找到，尝试通过官方名称 (Official Name) 查找
    if not country:
        try:
            country = pycountry.countries.get(official_name=normalized_name)
        except KeyError:
            pass

    # 如果仍然没有找到，尝试模糊匹配（可能会有多个结果，需谨慎处理）
    if not country:
        try:
            matches = pycountry.countries.search_fuzzy(normalized_name)
            if matches:
                # 简单选择第一个匹配项。
                # 在实际应用中，如果 matches 包含多个结果，你可能需要更复杂的启发式算法
                # 例如，选择编辑距离最小的，或者在已知国家列表中优先选择。
                country = matches[0]
                print(f"通过模糊匹配将 '{country_name_raw}' 映射到 '{country.name}'。")
        except LookupError: # pycountry 18.0.5+ fuzzy search raises LookupError if no results
            print(LookupError)
            pass

    if country:
        return country.alpha_2
    else:
        print(f"警告: 无法通过 pycountry 映射国家名称 '{country_name_raw}'。返回默认代码 '{default_code}'。")
        return default_code

