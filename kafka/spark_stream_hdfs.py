# File: spark_stream_hdfs.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType

# -----------------------------
# 1️⃣ SparkSession
# -----------------------------
spark = SparkSession.builder \
    .appName("KafkaTrafficLocalPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# 2️⃣ Kafka config
# -----------------------------
kafka_bootstrap = "127.0.0.1:9092"  # Kafka broker
kafka_topic = "traffic"

# -----------------------------
# 3️⃣ Read stream from Kafka
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
# 4️⃣ Define JSON schema
# -----------------------------
schema = StructType([
    StructField("camera", StringType()),
    StructField("timestamp", StringType()),
    StructField("counts", MapType(StringType(), IntegerType()))
])

df_parsed = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

# -----------------------------
# 5️⃣ Output paths (local)
# -----------------------------
output_path = "file:///D:/BigData/Simple_pipeline/traffic_data"
checkpoint_path = "file:///D:/BigData/Simple_pipeline/traffic_data_checkpoint"

# -----------------------------
# 6️⃣ Write to local folder (Parquet)
# -----------------------------
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="10 seconds") \
    .start()

# -----------------------------
# 7️⃣ Also log to console
# -----------------------------
console_query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# -----------------------------
# 8️⃣ Keep streaming
# -----------------------------
query.awaitTermination()
console_query.awaitTermination()
