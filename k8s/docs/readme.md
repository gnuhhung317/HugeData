## 1: setup

### run docker engine , minikube (nếu dùng minikube)

```bash
minikube start --driver=docker --memory 4096 --cpus 4
```

### setup local images
```bash
docker build -t spark-application:dev ./spark  
docker build -t kafka-producer:dev ./kafka  
```

### load images into minikube (nếu dùng minikube)
```bash
minikube image load kafka-producer:dev  
minikube image load spark-application:dev
```

### setup spark operator
```bash
helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator
helm repo update
```

### namespace and rbac for spark operator
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/spark-operator-complete-rbac.yaml -n hugedata
helm install spark-operator-1 spark-operator/spark-operator --namespace hugedata --set sparkJobNamespace=hugedata --set watchNamespace=hugedata --set webhook.enable=true
# đợi hơi lâu để spark operator chạy
```

### edit spark operator deployment
```bash
# (tìm namespaces=default đổi lại thành hugedata) 
kubectl edit deployment spark-operator-1-controller -n hugedata
```

## 2. start kafka / kafka producer / timescaledb
```bash
kubectl apply -f k8s/kafka.yaml

# realtime
kubectl apply -f k8s/timescaledb.yaml

# đợi kafka chạy xong
kubectl apply -f k8s/producer-deployment.yaml
```

### deploy grafana
```bash
# realtime
kubectl apply -f k8s/grafana.yaml
```

## 3. start hdfs
```bash
kubectl apply -f k8s/hdfs-cluster.yaml
```

## 4. setup timescaledb + grafana
### initialize timescaledb schema
```bash
# copy init.sql to pod
kubectl cp timescaledb/init.sql hugedata/timescaledb-0:/tmp/init.sql

# run init script
kubectl exec timescaledb-0 -n hugedata -- psql -U postgres -d traffic -f /tmp/init.sql
```

## 5. start spark
```bash
kubectl apply -f k8s/spark-batch-app.yaml -n hugedata
kubectl apply -f k8s/spark-realtime-app.yaml -n hugedata
kubectl apply -f k8s/spark-hdfs-reader.yaml -n hugedata
```

### verify data
```bash
# check record count
kubectl exec timescaledb-0 -n hugedata -- psql -U postgres -d traffic -c "SELECT COUNT(*) FROM traffic_metrics;"

# check latest data
kubectl exec timescaledb-0 -n hugedata -- psql -U postgres -d traffic -c "SELECT time, camera_id, total_count FROM traffic_metrics ORDER BY time DESC LIMIT 5;"
```


### access grafana
```bash
# port-forward grafana
kubectl port-forward svc/grafana 3000:3000 -n hugedata

# open browser: http://localhost:3000
# login: admin/admin
```
## port-forward services

### hdfs webview
```bash
kubectl port-forward pod/hdfs-namenode-0 9870:9870 -n hugedata
# open: http://localhost:9870
```

### timescaledb
```bash
kubectl port-forward svc/timescaledb 5432:5432 -n hugedata
# connect: psql -h localhost -U postgres -d traffic
```