import logging
import decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    get_json_object,
    to_date,
    unbase64,
    when,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    DecimalType,
)
from pyspark.sql.functions import udf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_to_s3")

# конфиги

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "pgserver.kafka.sales_events"
S3_BUCKET = "s3a://lake"
S3_OUTPUT = f"{S3_BUCKET}/sales_events"
S3_CHECKPOINT = f"{S3_BUCKET}/checkpoints/sales_events"
TRIGGER_SECONDS = 60

# UDF: декодирование Debezium Decimal (base64 bytes → decimal)
# Debezium кодирует NUMERIC как big-endian two's complement байты в base64


def debezium_bytes_to_decimal(b):
    if b is None:
        return None
    value = int.from_bytes(bytes(b), byteorder="big", signed=True)
    return decimal.Decimal(value) / decimal.Decimal(100)  # scale=2


decimal_udf = udf(debezium_bytes_to_decimal, DecimalType(10, 2))

# ─── схема payload (внутри обёртки {schema, payload}) ────────────────────────
# unit_price и total_price — StringType, т.к. в JSON они base64-строки

AFTER_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", StringType(), True),  # base64 bytes
        StructField("discount_pct", IntegerType(), True),
        StructField("total_price", StringType(), True),  # base64 bytes
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("is_returned", BooleanType(), True),
        StructField("rating", IntegerType(), True),
        StructField("created_at", LongType(), True),  # microseconds UTC
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("before", StringType(), True),
        StructField("after", AFTER_SCHEMA, True),
        StructField(
            "op", StringType(), True
        ),  # c=insert, u=update, d=delete, r=snapshot
        StructField("ts_ms", LongType(), True),
    ]
)

# SparkSession

spark = (
    SparkSession.builder.appName("KafkaToS3_SalesEvents")
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
logger.info("SparkSession started")

# чтение из Kafka

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# парсинг
# извлекаем только payload

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
    # извлекаем payload из обёртки Debezium
    .select(
        get_json_object(col("json_str"), "$.payload").alias("payload_str"),
        col("kafka_ts"),
    )
    .select(
        from_json(col("payload_str"), EVENT_SCHEMA).alias("e"),
        col("kafka_ts"),
    )
    .filter(col("e.op").isin("c", "r"))
    .filter(col("e.after.id").isNotNull())
    .select(
        col("e.after.id").alias("id"),
        col("e.after.order_id").alias("order_id"),
        col("e.after.customer_id").alias("customer_id"),
        col("e.after.product_id").alias("product_id"),
        col("e.after.product_name").alias("product_name"),
        col("e.after.category").alias("category"),
        col("e.after.quantity").alias("quantity"),
        # декодируем base64 - bytes - decimal
        decimal_udf(unbase64(col("e.after.unit_price"))).alias("unit_price"),
        col("e.after.discount_pct").alias("discount_pct"),
        decimal_udf(unbase64(col("e.after.total_price"))).alias("total_price"),
        col("e.after.region").alias("region"),
        col("e.after.city").alias("city"),
        col("e.after.payment_method").alias("payment_method"),
        col("e.after.channel").alias("channel"),
        col("e.after.is_returned").alias("is_returned"),
        col("e.after.rating").alias("rating"),
        # created_at из Postgres (microseconds - timestamp)
        from_unixtime(col("e.after.created_at") / 1_000_000).alias("event_time"),
        to_date(from_unixtime(col("e.after.created_at") / 1_000_000)).alias(
            "event_date"
        ),
        # CDC мета инфа
        col("e.op").alias("cdc_op"),
        (col("e.ts_ms") / 1000).cast("timestamp").alias("cdc_ts"),
        col("kafka_ts").alias("ingest_ts"),
        when(col("e.after.discount_pct") > 0, lit(True))
        .otherwise(lit(False))
        .alias("has_discount"),
    )
)

logger.info(f"Writing stream to {S3_OUTPUT}, partitioned by region/event_date")

# запись в Минио

query = (
    parsed_df.writeStream.format("parquet")
    .option("path", S3_OUTPUT)
    .option("checkpointLocation", S3_CHECKPOINT)
    .outputMode("append")
    .partitionBy("region", "event_date")
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .start()
)

logger.info("Stream query started, awaiting termination...")
query.awaitTermination()
