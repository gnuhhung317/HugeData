"""
spark batch job: hdfs reader -> cassandra writer

nhiệm vụ:
- đọc dữ liệu giao thông từ hdfs (raw data được lưu theo partition year/month/day)
- xử lý và tổng hợp dữ liệu theo các cửa sổ thời gian (30m, 1h, daily)
- ghi kết quả vào TẤT CẢ các bảng cassandra để phục vụ grafana dashboard

luồng xử lý:
1. đọc dữ liệu parquet từ hdfs theo ngày hiện tại
2. chuẩn hóa timestamp và filter dữ liệu hợp lệ
3. ghi raw events vào traffic_raw
4. tổng hợp theo cửa sổ 30 phút và 1 giờ (per camera + all cameras)
5. tổng hợp theo ngày (daily stats)
6. tổng hợp theo vehicle type (optional detailed breakdown)

output tables:
- traffic_raw: raw events per camera
- traffic_windowed_by_camera: per-camera windowed stats (30m, 1h)
- traffic_windowed_all: aggregated stats across all cameras (30m, 1h)
- traffic_hourly_by_camera: hourly stats per camera
- traffic_daily_by_camera: daily stats per camera
- traffic_vehicle_type_windowed: vehicle type breakdown (30m, 1h)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window, lit, to_date,
    sum as _sum, avg as _avg, max as _max, min as _min,
    struct, max as _max_struct, explode, array
)
from pyspark.sql.window import Window
import os
from datetime import datetime

# -------------------------------------------------
# Spark Batch: HDFS (raw) -> Cassandra (serving)
# -------------------------------------------------

def main():
    spark = (
        SparkSession.builder
        .appName("TrafficBatchToCassandra")
        # cassandra connector
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

    print(f"[INFO] reading HDFS path: {input_path}")

    # -------------------------------------------------
    # read raw data
    # -------------------------------------------------
    df = spark.read.parquet(input_path)

    # chuẩn hóa timestamp
    df = df.withColumn(
        "event_time",
        to_timestamp(col("time"))
    )

    # filter dữ liệu hợp lệ
    df = df.filter(col("camera_id").isNotNull())

    # -------------------------------------------------
    # 0. write raw events -> traffic_raw
    # -------------------------------------------------
    print("[INFO] writing raw events to traffic_raw...")
    
    # explode vehicle counts to individual events
    raw_events_df = (
        df.select(
            col("camera_id"),
            col("event_time"),
            explode(
                array(
                    *[struct(lit(vtype).alias("vehicle_type")) 
                      for vtype in ["car", "bus", "truck", "motorcycle"]]
                )
            ).alias("vehicle")
        )
        .select(
            col("camera_id"),
            col("event_time"),
            col("vehicle.vehicle_type")
        )
        .limit(10000)  # limit raw events to avoid too much data
    )

    (
        raw_events_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_raw"
        )
        .save()
    )

    # -------------------------------------------------
    # 1. windowed aggregation 30 minutes per camera
    # -------------------------------------------------
    print("[INFO] aggregating 30m windowed data per camera...")
    
    windowed_30m_df = (
        df.groupBy(
            col("camera_id"),
            window(col("event_time"), "30 minutes").alias("w")
        )
        .agg(
            _sum("car_count").alias("car_count"),
            _sum("bus_count").alias("bus_count"),
            _sum("truck_count").alias("truck_count"),
            _sum("motorcycle_count").alias("motorcycle_count"),
            _sum("total_count").alias("total_count")
        )
        .select(
            col("camera_id"),
            lit("30m").alias("window_type"),
            col("w.start").alias("window_start"),
            col("car_count"),
            col("bus_count"),
            col("truck_count"),
            col("motorcycle_count"),
            col("total_count")
        )
    )

    print("[INFO] writing 30m windowed data per camera to cassandra...")
    
    (
        windowed_30m_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_windowed_by_camera"
        )
        .save()
    )

    # -------------------------------------------------
    # 2. windowed aggregation 1 hour per camera
    # -------------------------------------------------
    print("[INFO] aggregating 1h windowed data per camera...")
    
    windowed_1h_df = (
        df.groupBy(
            col("camera_id"),
            window(col("event_time"), "1 hour").alias("w")
        )
        .agg(
            _sum("car_count").alias("car_count"),
            _sum("bus_count").alias("bus_count"),
            _sum("truck_count").alias("truck_count"),
            _sum("motorcycle_count").alias("motorcycle_count"),
            _sum("total_count").alias("total_count")
        )
        .select(
            col("camera_id"),
            lit("1h").alias("window_type"),
            col("w.start").alias("window_start"),
            col("car_count"),
            col("bus_count"),
            col("truck_count"),
            col("motorcycle_count"),
            col("total_count")
        )
    )

    print("[INFO] writing 1h windowed data per camera to cassandra...")
    
    (
        windowed_1h_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_windowed_by_camera"
        )
        .save()
    )

    # -------------------------------------------------
    # 3. aggregated windowed stats - all cameras (30m)
    # -------------------------------------------------
    print("[INFO] aggregating 30m data for all cameras...")
    
    all_cameras_30m_df = (
        windowed_30m_df.groupBy("window_start")
        .agg(
            _sum("total_count").alias("total_count")
        )
        .select(
            lit("30m").alias("window_type"),
            col("window_start"),
            col("total_count")
        )
    )

    print("[INFO] writing 30m aggregated data (all cameras) to cassandra...")
    
    (
        all_cameras_30m_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_windowed_all"
        )
        .save()
    )

    # -------------------------------------------------
    # 4. aggregated windowed stats - all cameras (1h)
    # -------------------------------------------------
    print("[INFO] aggregating 1h data for all cameras...")
    
    all_cameras_1h_df = (
        windowed_1h_df.groupBy("window_start")
        .agg(
            _sum("total_count").alias("total_count")
        )
        .select(
            lit("1h").alias("window_type"),
            col("window_start"),
            col("total_count")
        )
    )

    print("[INFO] writing 1h aggregated data (all cameras) to cassandra...")
    
    (
        all_cameras_1h_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_windowed_all"
        )
        .save()
    )

    # -------------------------------------------------
    # 5. hourly statistics per camera
    # -------------------------------------------------
    print("[INFO] calculating hourly statistics per camera...")
    
    hourly_stats_df = (
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
            col("w.start").alias("hour_start"),
            col("total_count"),
            col("avg_count"),
            col("max_count"),
            col("min_count")
        )
    )

    print("[INFO] writing hourly stats per camera to cassandra...")
    
    (
        hourly_stats_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_hourly_by_camera"
        )
        .save()
    )

    # -------------------------------------------------
    # 6. daily statistics per camera
    # -------------------------------------------------
    print("[INFO] calculating daily statistics per camera...")
    
    # find peak hour for each camera/day
    daily_with_peak_df = (
        df.withColumn("date", to_date(col("event_time")))
        .withColumn("hour", col("event_time"))
        .groupBy("camera_id", "date", "hour")
        .agg(_sum("total_count").alias("hour_total"))
    )

    # window function to find max hour per camera/day
    window_spec = Window.partitionBy("camera_id", "date").orderBy(col("hour_total").desc())
    
    daily_stats_df = (
        daily_with_peak_df
        .withColumn("rank", _max_struct(struct(col("hour_total"), col("hour"))).over(window_spec))
        .groupBy("camera_id", "date")
        .agg(
            _sum("hour_total").alias("total_count"),
            _max(struct(col("hour_total"), col("hour"))).alias("peak_info")
        )
        .select(
            col("camera_id"),
            col("date"),
            col("total_count"),
            col("peak_info.hour").alias("peak_hour"),
            col("peak_info.hour_total").alias("peak_count")
        )
    )

    print("[INFO] writing daily stats per camera to cassandra...")
    
    (
        daily_stats_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="traffic_data",
            table="traffic_daily_by_camera"
        )
        .save()
    )

    # -------------------------------------------------
    # 7. vehicle type windowed stats (30m)
    # -------------------------------------------------
    print("[INFO] calculating vehicle type windowed stats (30m)...")
    
    # create vehicle type breakdown for 30m
    vehicle_types = ["car", "bus", "truck", "motorcycle"]
    
    for vtype in vehicle_types:
        vtype_30m_df = (
            df.groupBy(
                col("camera_id"),
                window(col("event_time"), "30 minutes").alias("w")
            )
            .agg(
                _sum(f"{vtype}_count").alias("count")
            )
            .select(
                col("camera_id"),
                lit("30m").alias("window_type"),
                lit(vtype).alias("vehicle_type"),
                col("w.start").alias("window_start"),
                col("count")
            )
        )
        
        (
            vtype_30m_df.write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(
                keyspace="traffic_data",
                table="traffic_vehicle_type_windowed"
            )
            .save()
        )

    # -------------------------------------------------
    # 8. vehicle type windowed stats (1h)
    # -------------------------------------------------
    print("[INFO] calculating vehicle type windowed stats (1h)...")
    
    for vtype in vehicle_types:
        vtype_1h_df = (
            df.groupBy(
                col("camera_id"),
                window(col("event_time"), "1 hour").alias("w")
            )
            .agg(
                _sum(f"{vtype}_count").alias("count")
            )
            .select(
                col("camera_id"),
                lit("1h").alias("window_type"),
                lit(vtype).alias("vehicle_type"),
                col("w.start").alias("window_start"),
                col("count")
            )
        )
        
        (
            vtype_1h_df.write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(
                keyspace="traffic_data",
                table="traffic_vehicle_type_windowed"
            )
            .save()
        )

    print("[INFO] batch job completed successfully - all tables populated!")
    spark.stop()


if __name__ == "__main__":
    main()
