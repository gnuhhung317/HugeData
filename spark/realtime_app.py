import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# -----------------------------------------------------------
# Spark session (retain original _jsc Hadoop config semantics)
# -----------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("KafkaTrafficRealtimeApp")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------
# Logging setup
# ---------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("realtime_app")


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "traffic")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "spark-realtime-group")
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")

TRIGGER_TIMESCALE = os.environ.get("TRIGGER_TIMESCALE", "10 seconds")
TRIGGER_CONSOLE = os.environ.get("TRIGGER_CONSOLE", "5 seconds")

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "/app/data/checkpoint")
CHECKPOINT_TIMESCALE = os.path.join(CHECKPOINT_BASE, "traffic_timescaledb")
CHECKPOINT_CONSOLE = os.path.join(CHECKPOINT_BASE, "traffic_console")

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
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("kafka.group.id", KAFKA_GROUP_ID)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

df_string = df.selectExpr("CAST(value AS STRING)")
df_with_json = df_string.select(from_json(col("value"), schema).alias("data"), col("value").alias("raw_value"))
df_parsed = df_with_json.where(col("data").isNotNull()).select("data.*")

# ---------------------------------
# transform data for TimescaleDB
# ---------------------------------
df_valid = (
    df_parsed
    .withColumn("is_valid",
                (col("car_count") >= 0) & (col("bus_count") >= 0) &
                (col("truck_count") >= 0) & (col("motorcycle_count") >= 0) &
                (col("total_count") >= 0))
    .filter(col("is_valid"))
    .drop("is_valid")
)

df_timescale = df_valid.select(
    col("time").cast("timestamp").alias("time"),
    col("camera_id"),
    col("camera").alias("camera_name"),
    col("latitude"),
    col("longitude"),
    col("car_count"),
    col("motorcycle_count"),
    col("bus_count"),
    col("truck_count"),
    col("total_count"),
    current_timestamp().alias("ingest_ts")
)


# ---------------------------------
# Write to TimescaleDB (PostgreSQL)
# ---------------------------------
TIMESCALEDB_URL = os.environ.get("TIMESCALEDB_URL", "jdbc:postgresql://timescaledb.hugedata.svc.cluster.local:5432/traffic")
TIMESCALEDB_USER = os.environ.get("TIMESCALEDB_USER", "postgres")
TIMESCALEDB_PASSWORD = os.environ.get("TIMESCALEDB_PASSWORD", "postgres")
TIMESCALEDB_TABLE = os.environ.get("TIMESCALEDB_TABLE", "traffic_metrics")

def write_to_timescaledb(batch_df, batch_id):
    """Write each micro-batch to TimescaleDB with basic retries."""
    if batch_df is None:
        return

    # Cache once to avoid multiple actions
    batch_df_cached = batch_df.persist()
    row_count = batch_df_cached.count()
    if row_count == 0:
        return

    max_retries = int(os.environ.get("DB_WRITE_MAX_RETRIES", "3"))
    backoff_sec = float(os.environ.get("DB_WRITE_BACKOFF_SEC", "2"))

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            batch_df_cached.write \
                .format("jdbc") \
                .option("url", TIMESCALEDB_URL) \
                .option("dbtable", TIMESCALEDB_TABLE) \
                .option("user", TIMESCALEDB_USER) \
                .option("password", TIMESCALEDB_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info(f"Batch %s: wrote %s rows to TimescaleDB (attempt %s)", batch_id, row_count, attempt)
            last_err = None
            break
        except Exception as e:
            last_err = e
            logger.error(f"Batch %s: write failed (attempt %s/%s): %s", batch_id, attempt, max_retries, e)
            time.sleep(backoff_sec)

    if last_err is not None:
        logger.critical(f"Batch %s: failed to write after %s attempts: %s", batch_id, max_retries, last_err)

# Start TimescaleDB sink
try:
    timescale_query = (
        df_timescale.writeStream
        .outputMode("append")
        .queryName("timescale_sink")
        .foreachBatch(write_to_timescaledb)
        .option("checkpointLocation", CHECKPOINT_TIMESCALE)
        .trigger(processingTime=TRIGGER_TIMESCALE)
        .start()
    )
    logger.info("TimescaleDB sink started")
except Exception as e:
    logger.critical("Could not start TimescaleDB sink: %s", e)

# Also log to console for quick verification
try:
    console_query = (
        df.selectExpr("CAST(value AS STRING)")
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", CHECKPOINT_CONSOLE)
        .queryName("console_sink")
        .trigger(processingTime=TRIGGER_CONSOLE)
        .start()
    )
except Exception as e:
    logger.error("Could not start console sink: %s", e)

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    logger.info("Termination requested; stopping active streams...")
    for q in spark.streams.active:
        try:
            logger.info("Stopping query: %s", q.name)
            q.stop()
        except Exception as e:
            logger.error("Failed to stop query %s: %s", q.name, e)
    logger.info("Shutdown complete")


