import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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



kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.environ.get("KAFKA_TOPIC", "traffic")
kafka_group_id = os.environ.get("KAFKA_GROUP_ID", "spark-batch-group")

# ---------------------------------
# Define JSON schema for Kafka payload
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
# Read stream from Kafka
# ---------------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", kafka_topic)
    .option("kafka.group.id", kafka_group_id)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

df_string = df.selectExpr("CAST(value AS STRING)")
df_parsed = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")



# ---------------------------------
# HDFS Output Destination
# ---------------------------------
hdfs_namenode = os.environ.get("HDFS_NAMENODE", "hdfs://hdfs-namenode:8020")
hdfs_output_path = f"{hdfs_namenode}/traffic_data/traffic_stream"
hdfs_checkpoint_path = f"{hdfs_namenode}/traffic_data/checkpoint/traffic_stream"

try:
    (
        df_parsed.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", hdfs_output_path)
        .option("checkpointLocation", hdfs_checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"[HDFS-INFO] Parquet sink to HDFS started -> {hdfs_output_path}")
except Exception as e:
    print(f"[HDFS-WARN] Could not start Parquet HDFS sink. Error: {e}")

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


