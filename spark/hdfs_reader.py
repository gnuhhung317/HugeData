"""
spark batch job: hdfs reader -> cassandra writer

nhiệm vụ:
- đọc dữ liệu giao thông từ hdfs (raw data được lưu theo partition year/month/day)
- xử lý và tổng hợp dữ liệu theo 2 mức độ:
  1. realtime metrics: dữ liệu chi tiết từng camera theo timestamp
  2. hourly aggregation: tổng hợp theo giờ (sum, avg, max, min)
- ghi kết quả vào cassandra để phục vụ truy vấn và visualization

luồng xử lý:
1. đọc dữ liệu parquet từ hdfs theo ngày hiện tại
2. chuẩn hóa timestamp và filter dữ liệu hợp lệ
3. ghi dữ liệu realtime vào bảng traffic_metrics
4. tổng hợp theo cửa sổ 1 giờ
5. ghi dữ liệu tổng hợp vào bảng traffic_hourly

output:
- traffic_metrics: camera_id, timestamp, total_count, car_count, truck_count, bus_count, motorcycle_count
- traffic_hourly: camera_id, hour, total_count, avg_count, max_count, min_count
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window,
    sum as _sum, avg as _avg, max as _max, min as _min
)
import os
from datetime import datetime

# -------------------------------------------------
# Spark Batch: HDFS (raw) -> Cassandra (serving)
# -------------------------------------------------

def main():
    spark = (
        SparkSession.builder
        .appName("TrafficBatchToCassandra")
        # Cassandra connector
        .config(
            "spark.cassandra.connection.host",
            os.environ.get(
                "CASSANDRA_HOST",
                "cassandra.hugedata.svc.cluster.local"
            )
        )
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------------------------
    # HDFS input (incremental - theo ngày)
    # -------------------------------------------------
    hdfs_namenode = os.environ.get(
        "HDFS_NAMENODE",
        "hdfs://hdfs-namenode:8020"
    )

    # batch xử lý dữ liệu hôm nay
    today = datetime.utcnow()

    input_path = (
        f"{hdfs_namenode}/traffic_data/raw/traffic_stream/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}"
    )

    print(f"[INFO] Reading HDFS path: {input_path}")

    # -------------------------------------------------
    # Read raw data
    # -------------------------------------------------
    df = spark.read.parquet(input_path)

    # Chuẩn hóa timestamp
    df = df.withColumn(
        "event_time",
        to_timestamp(col("time"))
    )

    # -------------------------------------------------
    # 1. Write realtime-level data -> traffic_metrics
    # -------------------------------------------------
    metrics_df = (
        df.select(
            col("camera_id"),
            col("event_time").alias("timestamp"),
            col("total_count"),
            col("car_count"),
            col("truck_count"),
            col("bus_count"),
            col("motorcycle_count")
        )
        .filter(col("camera_id").isNotNull())
    )

    print("[INFO] wrote traffic_metrics to Cassandra")

    (
        metrics_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_metrics"
        )
        .save()
    )

    # -------------------------------------------------
    # 2. Hourly aggregation -> traffic_hourly
    # -------------------------------------------------
    hourly_df = (
        df.groupBy(
            col("camera_id"),
            window(col("event_time"), "1 hour").alias("w")
        )
        .agg(
            _sum("total_count").alias("total_count"),
            _avg("total_count").alias("avg_count"),
            _max("total_count").alias("max_count"),
            _min("total_count").alias("min_count")
        )
        .select(
            col("camera_id"),
            col("w.start").alias("hour"),
            col("total_count"),
            col("avg_count"),
            col("max_count"),
            col("min_count")
        )
    )

    print("[INFO] wrote traffic_hourly to Cassandra")

    (
        hourly_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_hourly"
        )
        .save()
    )

    print("[INFO] Batch job completed")
    spark.stop()


if __name__ == "__main__":
    main()
