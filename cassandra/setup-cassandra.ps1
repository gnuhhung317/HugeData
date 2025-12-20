# setup cassandra schema for traffic data
# this script creates a cassandra client pod and runs init.cql

$ErrorActionPreference = "Stop"

$NAMESPACE = "hugedata"
$CASSANDRA_HOST = "cassandra.hugedata.svc.cluster.local"
$CASSANDRA_PORT = "9042"
$INIT_FILE = "cassandra/init.cql"

Write-Host "checking if cassandra is ready..." -ForegroundColor Cyan
kubectl wait --for=condition=ready pod -l app=cassandra -n $NAMESPACE --timeout=300s

Write-Host "checking if cassandra-client pod already exists..." -ForegroundColor Cyan
$podExists = kubectl get pod cassandra-client -n $NAMESPACE --ignore-not-found
if ($podExists) {
    Write-Host "deleting existing cassandra-client pod..." -ForegroundColor Yellow
    kubectl delete pod cassandra-client -n $NAMESPACE --force --grace-period=0
    Start-Sleep -Seconds 5
}

Write-Host "creating cassandra-client pod..." -ForegroundColor Cyan
kubectl run cassandra-client `
    -n $NAMESPACE `
    --image=cassandra:3.11 `
    --restart=Never `
    --command -- sleep infinity

Write-Host "waiting for cassandra-client pod to be ready..." -ForegroundColor Cyan
kubectl wait --for=condition=ready pod/cassandra-client -n $NAMESPACE --timeout=60s

Write-Host "copying init.cql to cassandra-client pod..." -ForegroundColor Cyan
kubectl cp $INIT_FILE "$NAMESPACE/cassandra-client:/tmp/init.cql"

Write-Host "running init.cql script..." -ForegroundColor Cyan
kubectl exec -n $NAMESPACE cassandra-client -- `
    cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -f /tmp/init.cql

Write-Host "verifying schema..." -ForegroundColor Cyan
kubectl exec -n $NAMESPACE cassandra-client -- `
    cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e "DESCRIBE KEYSPACE traffic_data;"

Write-Host "`ncassandra setup completed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "to check data:" -ForegroundColor Yellow
Write-Host "  kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT COUNT(*) FROM traffic_metrics;`""
Write-Host ""
Write-Host "to delete cassandra-client pod:" -ForegroundColor Yellow
Write-Host "  kubectl delete pod cassandra-client -n $NAMESPACE"
