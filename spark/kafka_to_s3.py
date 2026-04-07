from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    BinaryType,
)

spark = (
    SparkSession.builder.appName("KafkaToS3")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# схема после поля payload (price - bytes, потому что NUMERIC в postgres)
schema = StructType(
    [
        StructField("before", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("product_id", IntegerType(), True),
                    StructField("category", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("price", BinaryType(), True),  # base64 -> bytes
                    StructField("region", StringType(), True),
                    StructField("created_at", LongType(), True),
                ]
            ),
            True,
        ),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ]
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "pgserver.kafka.sales_events")  # твой топик
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select(
        col("data.after.id").alias("id"),
        col("data.after.product_id").alias("product_id"),
        col("data.after.category").alias("category"),
        col("data.after.quantity").alias("quantity"),
        col("data.after.price").alias("price"),  # пока оставляем как bytes
        col("data.after.region").alias("region"),
        col("data.after.created_at").alias("created_at"),
        col("data.op").alias("op"),
        col("data.ts_ms").alias("cdc_ts"),
        current_timestamp().alias("ingest_ts"),
    )
    .filter(col("id").isNotNull())  # фильтруем пустые (например op="d")
)

query = (
    parsed.writeStream.format("parquet")
    .option("path", "s3a://lake/sales_events")
    .option("checkpointLocation", "s3a://lake/checkpoints/sales_events")
    .outputMode("append")
    .start()
)

query.awaitTermination()
