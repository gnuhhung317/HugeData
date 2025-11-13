# 1: setup
  # setup local image
    docker build -t spark-application:dev .
    docker build -t kafka-producer:dev .
    docker build -t minio-local:dev .

  # setup spark operator
  helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator
  helm repo update
  
  # tao namespace + apply quyen rbac cho spark operator
  kubectl create namespace hugedata
  kubectl apply -f spark-operator-complete-rbac.yaml -n hugedata
  helm install spark-operator-1 spark-operator/spark-operator --namespace hugedata --set watchNamespace=hugedata
  
  # edit deployment ( tim namespace: default doi thanh namespace: hugedata) 
  kubectl edit deployment spark-operator-1-controller -n hugedata

  # restart spark operator ( sau khi install, neu muon khoi dong lai)
  helm upgrade spark-operator-1 spark-operator/spark-operator   --namespace hugedata   --set watchNamespace=hugedata


# 1. start kafka / kafka producer
kubectl apply -f kafka.yaml
kubectl apply -f producer-deployment.yaml

# 2. start minio
kubectl apply -f minio-deployment.yaml
kubectl apply -f minio-bucket.yaml

# 3 . start spark
kubectl apply -f spark-app.yaml -n hugedata

# 4. clean
kubectl delete deployment --all -n hugedata
kubectl delete pod spark-pi-python-driver -n hugedata
kubectl scale statefulset kafka -n hugedata --replicas=0


# debug 
kubectl describe sparkapplication spark-pi-python -n hugedata
kubectl logs spark-pi-python-driver -n hugedata