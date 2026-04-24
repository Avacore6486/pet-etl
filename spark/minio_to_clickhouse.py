import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("minio_to_clickhouse")

# дата передаётся из Airflow как аргумент: YYYY-MM-DD
if len(sys.argv) < 2:
    raise ValueError("Передай дату: spark-submit minio_to_clickhouse.py 2024-01-15")

execution_date = sys.argv[1]
logger.info(f"Загружаем данные за дату: {execution_date}")

S3_INPUT = "s3a://lake/sales_events"
CH_HOST = "clickhouse01"
CH_PORT = "8123"
CH_DATABASE = "default"
CH_TABLE = "raw_sales_events_dist"
CH_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DATABASE}"

spark = (
    SparkSession.builder.appName("MinioToClickHouse_SalesEvents")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# читаем все партиции за нужную дату (partition pruning по event_date)
df = spark.read.parquet(S3_INPUT).filter(f"event_date = '{execution_date}'")

count = df.count()
logger.info(f"Прочитано строк из MinIO: {count}")

if count == 0:
    logger.info("Нет данных за эту дату, завершаем.")
    spark.stop()
    sys.exit(0)

# пишем в ClickHouse через JDBC
(
    df.write.format("jdbc")
    .option("url", CH_URL)
    .option("dbtable", CH_TABLE)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("user", "default")
    .option("password", "")
    .option("batchsize", "10000")
    .mode("append")
    .save()
)

logger.info(f"Загрузка завершена: {count} строк записано в {CH_TABLE}")
spark.stop()
