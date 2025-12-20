import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# -----------------------------------------------------------
# Spark session
# -----------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("KafkaTrafficToHDFS")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.environ.get("KAFKA_TOPIC", "traffic")

# ---------------------------------
# JSON schema
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
# Read from Kafka
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

df_parsed = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
)

# ---------------------------------
# Parse time & create partitions
# ---------------------------------
df_with_time = (
    df_parsed
    .withColumn("event_time", to_timestamp(col("time")))
    .withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("day", dayofmonth(col("event_time")))
    .withColumn("hour", hour(col("event_time")))
)

# ---------------------------------
# HDFS output (partitioned)
# ---------------------------------
hdfs_namenode = os.environ.get("HDFS_NAMENODE", "hdfs://hdfs-namenode:8020")

hdfs_output_path = f"{hdfs_namenode}/traffic_data/raw/traffic_stream"
hdfs_checkpoint_path = f"{hdfs_namenode}/traffic_data/checkpoints/traffic_stream"

(
    df_with_time.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", hdfs_output_path)
    .option("checkpointLocation", hdfs_checkpoint_path)
    .partitionBy("year", "month", "day", "hour")
    # Spark thực hiện gom data trong 2 phút mỗi lần ghi vào hdfs 
    .trigger(processingTime="2 minutes") 
    .start()
)

spark.streams.awaitAnyTermination()
