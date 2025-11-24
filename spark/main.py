import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType, DoubleType

# -----------------------------------------------------------
# Spark session (retain original _jsc Hadoop config semantics)
# -----------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("KafkaTrafficLocalTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Keep the _jsc Hadoop configuration EXACTLY like the previous simple version
# (no additional bucket-scoped overrides or extra keys beyond what existed).
def load_config(sc):
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

load_config(spark.sparkContext)

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.environ.get("KAFKA_TOPIC", "traffic")

# ---------------------------------
# Define JSON schema for Kafka payload
# ---------------------------------
schema = StructType([
    StructField("time", StringType()),
    StructField("camera_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("camera", StringType()),
    StructField("timestamp", StringType()),
    StructField("car_count", IntegerType()),
    StructField("bus_count", IntegerType()),
    StructField("truck_count", IntegerType()),
    StructField("motorcycle_count", IntegerType()),
    StructField("total_count", IntegerType()),
])

# ---------------------------------
# Read stream from Kafka
# ---------------------------------
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
df_parsed = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

# ---------------------------------
# Output destinations
# ---------------------------------
bucket_name = os.environ.get("MINIO_BUCKET", "traffic-data")
output_path = f"s3a://{bucket_name}/traffic_stream"
checkpoint_path = f"s3a://{bucket_name}/checkpoint/traffic_stream"

# Start S3A sink; on failure, fallback to local FS inside container
try:
    (
        df_parsed.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"[INFO] Parquet sink to S3A started -> {output_path}")
except Exception as e:
    print("[WARN] Could not start Parquet S3A sink. Falling back to local FS. Error:", e)
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
    print(f"[INFO] Fallback Parquet sink started -> {local_output_path}")

# Also log to console for quick verification
(
    df.selectExpr("CAST(value AS STRING)")
    .writeStream
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")
    .start()
)

spark.streams.awaitAnyTermination()


