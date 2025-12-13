#!/bin/bash

NAMESPACE="hugedata"

echo "===== CLEANING SPARK JOBS ====="
kubectl delete -f k8s/spark-batch-app.yaml -n $NAMESPACE --ignore-not-found
kubectl delete -f k8s/spark-realtime-app.yaml -n $NAMESPACE --ignore-not-found
kubectl delete -f k8s/spark-hdfs-reader.yaml -n $NAMESPACE --ignore-not-found

echo "===== CLEANING HDFS ====="
kubectl delete -f k8s/hdfs-cluster.yaml --ignore-not-found

echo "===== CLEANING KAFKA / PRODUCER ====="
kubectl delete -f k8s/producer-deployment.yaml --ignore-not-found
kubectl delete -f k8s/kafka.yaml --ignore-not-found

echo "===== CLEANING TIMESCALEDB ====="
kubectl delete -f k8s/timescaledb.yaml --ignore-not-found

echo "===== CLEANING GRAFANA ====="
kubectl delete -f k8s/grafana.yaml --ignore-not-found

echo "===== CLEANING SPARK OPERATOR ====="
kubectl delete deployment spark-operator-1-webhook -n $NAMESPACE
kubectl delete deployment spark-operator-1-controller -n $NAMESPACE
echo "===== CLEANING NAMESPACE (OPTIONAL) ====="
# uncomment nếu muốn xóa luôn namespace
# kubectl delete namespace $NAMESPACE

echo "===== DONE ====="
