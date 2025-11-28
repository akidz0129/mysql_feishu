import subprocess
import sys

# 你传入的模块名，比如 "iceberg", "s3", "mysql"
job_type = sys.argv[1]

jar_map = {
    "classify_raw_data": "/app/external_jars/spark-excel_2.12-3.5.1_0.20.4.jar,/app/external_jars/hadoop-aws-3.3.4.jar,/app/external_jars/aws-java-sdk-bundle-1.12.528.jar",
    "standardize_orders": "/app/external_jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,/app/external_jars/mysql-connector-j-9.3.0.jar,/app/external_jars/hadoop-aws-3.4.1.jar,/app/external_jars/bundle-2.32.4.jar",
    "extract": "/app/external_jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,/app/external_jars/mysql-connector-j-9.3.0.jar,/app/external_jars/bundle-2.32.4.jar,/app/external_jars/spark-excel_2.12-3.5.1_0.20.4.jar,/app/external_jars/hadoop-aws-3.3.4.jar,/app/external_jars/aws-java-sdk-bundle-1.12.528.jar",
}

# 获取所需 JAR
jars = jar_map.get(job_type)
if jars is None:
    print(f"Unknown job type: {job_type}")
    sys.exit(1)

# 拼接 spark-submit 命令
cmd = [
    "/opt/spark/bin/spark-submit",
    "--master", "spark://spark-master:7077",
    "--deploy-mode", "client",
    "--jars", jars,
    f"/app/app/{job_type}.py"  # 你的主作业脚本路径
]

# 调用
subprocess.run(cmd)
