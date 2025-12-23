import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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
# def load_config(sc):
#     sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
#     sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
#     sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
#     sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
#     sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

# load_config(spark.sparkContext)

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
kafka_topic = os.environ.get("KAFKA_TOPIC", "traffic")
kafka_group_id = os.environ.get("KAFKA_GROUP_ID", "spark-realtime-group")

# ---------------------------------
# define JSON schema for Kafka payload (matching producer.py format)
# ---------------------------------
schema = StructType([
    StructField("time", StringType()),
    StructField("camera_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("camera", StringType()),
    StructField("car_count", IntegerType()),
    StructField("bus_count", IntegerType()),
    StructField("truck_count", IntegerType()),
    StructField("motorcycle_count", IntegerType()),
    StructField("total_count", IntegerType()),
])

# ---------------------------------
# read stream from Kafka
# ---------------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest") 
    .option("kafka.max.partition.fetch.bytes", "1048576")  # 1MB
    .option("kafka.fetch.max.bytes", "5242880")            # 5MB
    .option("maxOffsetsPerTrigger", "2000")
    .option("failOnDataLoss", "false")
    .load()
)

df_string = df.selectExpr("CAST(value AS STRING)")
df_parsed = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

# ---------------------------------
# transform data for TimescaleDB
# ---------------------------------
df_with_parsed_time = df_parsed.withColumn(
    "parsed_time",
    to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")
)

# If parsing fails, optionally fall back to Kafka ingestion time or current time
# Note: Using current_timestamp() keeps the stream alive but may slightly skew analytics
df_with_safe_time = df_with_parsed_time.withColumn(
    "safe_time",
    when(col("parsed_time").isNotNull(), col("parsed_time")).otherwise(current_timestamp())
)

# Split into good/bad rows based on timestamp parsing
df_good = df_with_safe_time.filter(col("parsed_time").isNotNull())
df_bad = df_with_safe_time.filter(col("parsed_time").isNull())

# Project good rows to target schema
df_timescale = df_good.select(
    col("safe_time").alias("time"),
    col("camera_id"),
    col("camera").alias("camera_name"),
    col("latitude"),
    col("longitude"),
    col("car_count"),
    col("motorcycle_count"),
    col("bus_count"),
    col("truck_count"),
    col("total_count")
)

# Log bad rows to console for diagnosis (without breaking the stream)
(df_bad
    .selectExpr("CAST(struct(*) AS STRING) AS value")
    .writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .trigger(processingTime="10 seconds")
    .start()
)

# ---------------------------------
# Output destinations
# ---------------------------------
# bucket_name = os.environ.get("MINIO_BUCKET", "traffic-data")
# output_path = f"s3a://{bucket_name}/traffic_stream"
# checkpoint_path = f"s3a://{bucket_name}/checkpoint/traffic_stream"

# # Start S3A sink; on failure, fallback to local FS inside container
# try:
#     (
#         df_parsed.writeStream
#         .outputMode("append")
#         .format("parquet")
#         .option("path", output_path)
#         .option("checkpointLocation", checkpoint_path)
#         .trigger(processingTime="10 seconds")
#         .start()
#     )
#     print(f"[INFO] Parquet sink to S3A started -> {output_path}")
# except Exception as e:
#     print("[WARN] Could not start Parquet S3A sink. Falling back to local FS. Error:", e)
#     local_output_path = "/app/data/traffic_stream"
#     local_checkpoint_path = "/app/data/checkpoint/traffic_stream"
#     (
#         df_parsed.writeStream
#         .outputMode("append")
#         .format("parquet")
#         .option("path", local_output_path)
#         .option("checkpointLocation", local_checkpoint_path)
#         .trigger(processingTime="10 seconds")
#         .start()
#     )
#     print(f"[INFO] Fallback Parquet sink started -> {local_output_path}")

# ---------------------------------
# Write to TimescaleDB (PostgreSQL)
# ---------------------------------
timescaledb_url = os.environ.get("TIMESCALEDB_URL", "jdbc:postgresql://timescaledb.hugedata.svc.cluster.local:5432/traffic")
timescaledb_user = os.environ.get("TIMESCALEDB_USER", "postgres")
timescaledb_password = os.environ.get("TIMESCALEDB_PASSWORD", "postgres")
timescaledb_table = os.environ.get("TIMESCALEDB_TABLE", "traffic_metrics")

def write_to_timescaledb(batch_df, batch_id):
    """Write each batch to TimescaleDB"""
    if batch_df.count() > 0:
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", timescaledb_url) \
                .option("dbtable", timescaledb_table) \
                .option("user", timescaledb_user) \
                .option("password", timescaledb_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"[INFO] Batch {batch_id}: Written {batch_df.count()} records to TimescaleDB")
        except Exception as e:
            print(f"[ERROR] Batch {batch_id}: Failed to write to TimescaleDB - {e}")

# Start TimescaleDB sink
checkpoint_timescale = "/app/data/checkpoint/traffic_timescaledb"
try:
    (
        df_timescale.writeStream
        .foreachBatch(write_to_timescaledb)
        .option("checkpointLocation", checkpoint_timescale)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"[INFO] TimescaleDB sink started")
except Exception as e:
    print(f"[ERROR] Could not start TimescaleDB sink: {e}")

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


