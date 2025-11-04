
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType, DoubleType

# -----------------------------
# SparkSession
# -----------------------------
# For local: uncomment config line below
# For K8s: packages already in spark-submit args
# IMPORTANT: s3a.endpoint should NOT include the scheme. Provide host:port only.
minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio.hugedata.svc.cluster.local:9000")
# Ensure endpoint includes scheme for AWS SDK; S3A accepts http(s)://host:port in newer Hadoop versions
if not minio_endpoint.startswith("http://") and not minio_endpoint.startswith("https://"):
    endpoint_with_scheme = f"http://{minio_endpoint}"
else:
    endpoint_with_scheme = minio_endpoint
minio_access = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
minio_secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

# Use numeric byte values for sizes — some Hadoop/S3A settings expect plain numbers and
# will throw NumberFormatException for values like "64M". Setting these explicitly
# avoids the Java parsing error seen during stream start.
spark = (SparkSession.builder
    .appName("KafkaTrafficLocalPipeline")
    .config("spark.jars.packages", 
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ]))
    # Pre-seed Hadoop configs at builder time so S3A picks them up early
    .config("spark.hadoop.fs.s3a.endpoint", endpoint_with_scheme)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", minio_access)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.bucket.traffic-data.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.bucket.traffic-data.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.bucket.traffic-data.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.bucket.traffic-data.access.key", minio_access)
    .config("spark.hadoop.fs.s3a.bucket.traffic-data.secret.key", minio_secret)
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
    .config("spark.hadoop.fs.s3a.multipart.threshold", "67108864")
    .config("spark.hadoop.fs.s3a.block.size", "33554432")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# Set endpoint with http scheme to ensure path-style handling in AWS SDK
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", endpoint_with_scheme)
# Provide credentials explicitly to avoid provider resolution issues in-cluster
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_access)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_secret)
# Ensure static credentials provider and disable SSL when using http endpoint
spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Bucket-specific overrides to force path-style and correct endpoint
bucket_name = "traffic-data"
spark.sparkContext._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{bucket_name}.endpoint", endpoint_with_scheme)
spark.sparkContext._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{bucket_name}.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{bucket_name}.connection.ssl.enabled", "false")
spark.sparkContext._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{bucket_name}.access.key", minio_access)
spark.sparkContext._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{bucket_name}.secret.key", minio_secret)

# ---- Debug: print effective S3A config ----
conf = spark.sparkContext._jsc.hadoopConfiguration()
print("[DEBUG] fs.s3a.endpoint:", conf.get("fs.s3a.endpoint"))
print("[DEBUG] fs.s3a.path.style.access:", conf.get("fs.s3a.path.style.access"))
print("[DEBUG] fs.s3a.connection.ssl.enabled:", conf.get("fs.s3a.connection.ssl.enabled"))
print("[DEBUG] fs.s3a.aws.credentials.provider:", conf.get("fs.s3a.aws.credentials.provider"))
print("[DEBUG] fs.s3a.bucket.traffic-data.endpoint:", conf.get("fs.s3a.bucket.traffic-data.endpoint"))
print("[DEBUG] fs.s3a.bucket.traffic-data.path.style.access:", conf.get("fs.s3a.bucket.traffic-data.path.style.access"))
print("[DEBUG] fs.s3a.bucket.traffic-data.connection.ssl.enabled:", conf.get("fs.s3a.bucket.traffic-data.connection.ssl.enabled"))
try:
    import sys
    sys.stdout.flush()
except Exception:
    pass

# -----------------------------
# Kafka config
# -----------------------------
# kafka_bootstrap = "localhost:9092"

# Kubernetes deployment:
kafka_bootstrap = "kafka.hugedata.svc.cluster.local:9092"

kafka_topic = "traffic"

# -----------------------------
# Read stream from Kafka
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert value from bytes to string
df_string = df.selectExpr("CAST(value AS STRING)")

# -----------------------------
# Define JSON schema
# -----------------------------
schema = StructType([
    StructField("camera", StringType()),
    StructField("camera_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("counts", MapType(StringType(), IntegerType()))
])

df_parsed = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

# -----------------------------
# Output paths
# -----------------------------
# For Kubernetes with MinIO (S3A)
output_path = "s3a://traffic-data/traffic_stream"
checkpoint_path = "s3a://traffic-data/checkpoint/traffic_stream"

# -----------------------------
# Write to local folder (Parquet)
# -----------------------------
# Start S3 sink if available; don't crash the app if MinIO/S3A is misconfigured
try:
    query = (df_parsed.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    print("[INFO] Parquet sink to S3A started ->", output_path)
except Exception as e:
    print("[WARN] Could not start Parquet S3A sink. Falling back to local FS. Error:", e)
    try:
        local_output_path = "/app/data/traffic_stream"
        local_checkpoint_path = "/app/data/checkpoint/traffic_stream"
        (df_parsed.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", local_output_path)
            .option("checkpointLocation", local_checkpoint_path)
            .trigger(processingTime="10 seconds")
            .start())
        print("[INFO] Fallback Parquet sink started ->", local_output_path)
    except Exception as e2:
        print("[ERROR] Fallback local Parquet sink failed:", e2)

# -----------------------------
# Also log to console
# -----------------------------
df.selectExpr("CAST(value AS STRING)").writeStream \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()


# -----------------------------
# Keep streaming
# -----------------------------
spark.streams.awaitAnyTermination()
