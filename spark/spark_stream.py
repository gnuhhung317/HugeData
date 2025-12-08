import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    MapType, IntegerType, DoubleType
)


# ============================================================
# Helper functions for MinIO / S3A config
# ============================================================
def load_minio_env():
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio.hugedata.svc.cluster.local:9000")
    
    # Add scheme if missing
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        endpoint_with_scheme = f"http://{endpoint}"
    else:
        endpoint_with_scheme = endpoint

    return {
        "endpoint": endpoint,
        "endpoint_with_scheme": endpoint_with_scheme,
        "access": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "secret": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    }


def apply_s3a_base_config(spark, cfg):
    c = spark.sparkContext._jsc.hadoopConfiguration()
    c.set("fs.s3a.endpoint", cfg["endpoint_with_scheme"])
    c.set("fs.s3a.path.style.access", "true")
    c.set("fs.s3a.connection.ssl.enabled", "false")
    c.set("fs.s3a.access.key", cfg["access"])
    c.set("fs.s3a.secret.key", cfg["secret"])
    c.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    c.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def apply_bucket_config(spark, bucket, cfg):
    c = spark.sparkContext._jsc.hadoopConfiguration()
    prefix = f"fs.s3a.bucket.{bucket}"
    c.set(f"{prefix}.endpoint", cfg["endpoint_with_scheme"])
    c.set(f"{prefix}.path.style.access", "true")
    c.set(f"{prefix}.connection.ssl.enabled", "false")
    c.set(f"{prefix}.access.key", cfg["access"])
    c.set(f"{prefix}.secret.key", cfg["secret"])


# ============================================================
# SparkSession
# ============================================================
cfg = load_minio_env()
bucket_name = "traffic-data"

spark = (
    SparkSession.builder
    .appName("KafkaTrafficLocalPipeline")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )
    # Pre-seed MinIO configs
    .config("spark.hadoop.fs.s3a.endpoint", cfg["endpoint_with_scheme"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", cfg["access"])
    .config("spark.hadoop.fs.s3a.secret.key", cfg["secret"])
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Bucket overrides
    .config(f"spark.hadoop.fs.s3a.bucket.{bucket_name}.endpoint", cfg["endpoint_with_scheme"])
    .config(f"spark.hadoop.fs.s3a.bucket.{bucket_name}.path.style.access", "true")
    .config(f"spark.hadoop.fs.s3a.bucket.{bucket_name}.connection.ssl.enabled", "false")
    .config(f"spark.hadoop.fs.s3a.bucket.{bucket_name}.access.key", cfg["access"])
    .config(f"spark.hadoop.fs.s3a.bucket.{bucket_name}.secret.key", cfg["secret"])
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Apply Hadoop config corrections
apply_s3a_base_config(spark, cfg)
apply_bucket_config(spark, bucket_name, cfg)

# Debug print S3A config
conf = spark.sparkContext._jsc.hadoopConfiguration()
debug_keys = [
    "fs.s3a.endpoint",
    "fs.s3a.path.style.access",
    "fs.s3a.connection.ssl.enabled",
    "fs.s3a.aws.credentials.provider",
    f"fs.s3a.bucket.{bucket_name}.endpoint",
    f"fs.s3a.bucket.{bucket_name}.path.style.access",
    f"fs.s3a.bucket.{bucket_name}.connection.ssl.enabled",
]

print("\n[DEBUG] ---- Effective S3A Config ----")
for key in debug_keys:
    print(f"{key}: {conf.get(key)}")
print("-------------------------------------\n")


# ============================================================
# Kafka config
# ============================================================
kafka_bootstrap = "kafka.hugedata.svc.cluster.local:9092"
kafka_topic = "traffic"


# ============================================================
# Kafka Stream
# ============================================================
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

df_string = df.selectExpr("CAST(value AS STRING)")


# ============================================================
# JSON schema
# ============================================================
schema = StructType([
    StructField("camera", StringType()),
    StructField("camera_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("counts", MapType(StringType(), IntegerType()))
])

df_parsed = df_string.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")


# ============================================================
# Output (S3A → Parquet)
# ============================================================
output_path = "s3a://traffic-data/traffic_stream"
checkpoint_path = "s3a://traffic-data/checkpoint/traffic_stream"

try:
    query = (
        df_parsed.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print("[INFO] Parquet sink to S3A started ->", output_path)

except Exception as e:
    print("[WARN] Could not start Parquet S3A sink. Falling back to local FS. Error:", e)
    
    try:
        local_output_path = "/app/data/traffic_stream"
        local_checkpoint_path = "/app/data/checkpoint/traffic_stream"

        (
            df_parsed.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", local_output_path)
            .option("checkpointLocation", local_checkpoint_path)
            .trigger(processingTime="10 seconds")
            .start()
        )
        print("[INFO] Fallback Parquet sink started ->", local_output_path)

    except Exception as e2:
        print("[ERROR] Fallback local Parquet sink failed:", e2)


# ============================================================
# Console sink
# ============================================================
df.selectExpr("CAST(value AS STRING)").writeStream \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()


# ============================================================
# Keep streaming
# ============================================================
spark.streams.awaitAnyTermination()
