## 1: setup

### run docker engine , minikube (nếu dùng minikube)

```bash
minikube start --driver=docker --memory 4096 --cpus 4
```

### setup local images
```bash
# tải thủ công các thư viện .jar trong requirements.txt
# hoặc đổi file dockerfile trong ./legacy/ vào thư mục spark để tải trực tiếp vào image.
docker build -t spark-application:dev ./spark  
docker build -t kafka-producer:dev ./kafka  
docker build -t minio-local:dev ./minio  
```

### load images into minikube (nếu dùng minikube)
```bash
minikube image load kafka-producer:dev  
minikube image load spark-application:dev
minikube image load minio-local:dev  # Chưa cần dùng đến, bỏ qua
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

## 2. start kafka / kafka producer
```bash
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/producer-deployment.yaml
```

## 3. start minio / hdfs
```bash
kubectl apply -f k8s/hdfs/hdfs-cluster.yaml

kubectl apply -f k8s/minio-deployment.yaml
kubectl apply -f k8s/minio-bucket.yaml
```

## 4. start spark
```bash
kubectl apply -f k8s/spark-app.yaml -n hugedata
kubectl apply -f k8s/spark-hdfs-reader.yaml -n hugedata
```

## clean (optional)
```bash
kubectl delete deployment --all -n hugedata
kubectl delete pod spark-pi-python-driver -n hugedata
kubectl scale statefulset kafka -n hugedata --replicas=0
```

## debug (optional)
```bash
kubectl describe sparkapplication spark-pi-python -n hugedata
kubectl logs spark-pi-python-driver -n hugedata
```

## port-forward minio webview
```bash
kubectl port-forward deployment/minio 9001:9001 9000:9000 -n hugedata
```
## port-forward hdfs webview
```bash
kubectl port-forward deployment/hdfs-namenode 9870:9870 -n hugedata
```
```