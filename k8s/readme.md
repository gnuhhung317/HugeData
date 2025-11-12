# 1: setup
  # setup spark operator
  helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator
  helm repo update
  
  # tao namespace + apply quyen rbac cho spark operator
  kubectl create namespace hugedata
  kubectl apply -f spark-operator-complete-rbac.yaml -n hugedata
  helm install spark-operator-1 spark-operator/spark-operator --namespace hugedata --set watchName
  space=hugedata
  
  # edit deployment ( tim namespace: default doi thanh namespace: hugedata) 
  kubectl edit deployment spark-operator-1-controller -n hugedata

  # chay spark app 
  kubectl apply -f spark-app.yaml -n hugedata
  



# 2. start kafka 

# 3. start minio

# 4. clean
kubectl delete deployment --all -n hugedata
kubectl delete SparkApplication spark-pi-python
kubectl delete pod spark-pi-python-driver -n hugedata

# restart spark operator
helm upgrade spark-operator-1 spark-operator/spark-operator   --namespace hugedata   --set watchName
space=hugedata


# debug 
kubectl describe sparkapplication spark-pi-python -n hugedata
kubectl logs spark-pi-python-driver -n hugedata