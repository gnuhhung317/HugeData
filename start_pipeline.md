# start broker
docker compose up
# start spark
 spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 D:\BigData\Simple_pipeline\spark\spark_stream.py