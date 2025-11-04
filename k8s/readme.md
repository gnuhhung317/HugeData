# 0 .pull image local 
docker pull octoenergy/pyspark:3.5.2
docker pull ultralytics/ultralytics:latest-python
docker pull minio/minio:latest



# 1. build image local ( neu ko pull ve)( chuyen sang thu muc chua dockerfile truoc khi build)
docker build -t kafka-producer:dev .
docker build -t spark-streaming:dev .
docker build -t minio-local:dev .



# 2
.scripts/start_pipeline.sh


# Liệt kê tất cả pods trong namespace
kubectl get pods -n hugedata

# Xem trạng thái chi tiết của pod MinIO
kubectl describe pod -n hugedata -l app=minio

# Xem logs pod MinIO
kubectl logs -n hugedata -l app=minio

# Kiểm tra Spark streaming pod
kubectl describe pod -n hugedata -l app=spark-streaming
kubectl logs -n hugedata -l app=spark-streaming

# Kiểm tra Kafka producer pod
kubectl describe pod -n hugedata -l app=kafka-producer
kubectl logs -n hugedata -l app=kafka-producer

# Clean / stop 
.scripts/clean.sh

