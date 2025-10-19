# File: spark_stream_hdfs.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType

# -----------------------------
# SparkSession
# -----------------------------
spark = SparkSession.builder \
    .appName("KafkaTrafficLocalPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Kafka config
# -----------------------------
kafka_bootstrap = "localhost:9092"
kafka_topic = "traffic"

# -----------------------------
# Read stream from Kafka
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Convert value from bytes to string
df_string = df.selectExpr("CAST(value AS STRING)")

# -----------------------------
# Define JSON schema
# -----------------------------
schema = StructType([
    StructField("camera", StringType()),
    StructField("timestamp", StringType()),
    StructField("counts", MapType(StringType(), IntegerType()))
])

df_parsed = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

# -----------------------------
# Output paths (local)
# -----------------------------
output_path = "file:///app/data"
checkpoint_path = "file:///app/data/checkpoint"

# -----------------------------
# Write to local folder (Parquet)
# -----------------------------
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="10 seconds") \
    .start()

# -----------------------------
# Also log to console
# -----------------------------
console_query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# -----------------------------
# Keep streaming
# -----------------------------
query.awaitTermination()
console_query.awaitTermination()
