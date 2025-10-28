
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType, DoubleType

# -----------------------------
# SparkSession
# -----------------------------
# For local: uncomment config line below
# For K8s: packages already in spark-submit args
spark = SparkSession.builder \
    .appName("KafkaTrafficLocalPipeline") \
    .getOrCreate()
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \

spark.sparkContext.setLogLevel("WARN")

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
    .option("startingOffsets", "latest") \
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
# Output paths (local)
# -----------------------------
# Local testing:
# output_path = "file:///app/data"
# checkpoint_path = "file:///app/data/checkpoint"

# Kubernetes deployment:
output_path = "/app/data/traffic_stream"
checkpoint_path = "/app/data/checkpoint"

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
spark.streams.awaitAnyTermination()
