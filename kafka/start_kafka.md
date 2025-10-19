# create broker (server)
& "C:\Users\hungc\kafka_2.13-4.1.0\bin\windows\kafka-server-start.bat" "C:\Users\hungc\kafka_2.13-4.1.0\config\server.properties"

# run server
& "C:\Users\hungc\kafka_2.13-4.1.0\bin\windows\kafka-server-start.bat" "C:\Users\hungc\kafka_2.13-4.1.0\config\server.properties"

# create topic
kafka-topics.bat --create --topic traffic --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 1 

# start spark
C:\Users\hungc\spark-4.0.1-bin-hadoop3\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 D:\BigData\Simple_pipeline\spark_stream_hdfs.py

