from pyspark.sql import SparkSession
import os
# Triển khai lấy data từ HDFS bằng Spark batch
# Xem log: kubectl logs spark-hdfs-reader-driver -n hugedata

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("HDFSReaderBatch") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    hdfs_namenode = os.environ.get("HDFS_NAMENODE", "hdfs://hdfs-namenode:8020")
    input_path = f"{hdfs_namenode}/traffic_data/traffic_stream"

    print(f"------------------------------------------------")
    print(f"Attempting to read data from: {input_path}")
    print(f"------------------------------------------------")

    try:
        df = spark.read.parquet(input_path)
        
        # Show schema
        print("Schema:")
        df.printSchema()

        # Show row count
        count = df.count()
        print(f"Total rows found: {count}")

        # Show sample data
        print("Sample data (top 20):")
        df.show(truncate=False)

    except Exception as e:
        print(f"Error reading from HDFS: {e}")
        # Keep the pod alive briefly to allow log inspection if it fails immediately
        import time
        time.sleep(10)

    spark.stop()

if __name__ == "__main__":
    main()
