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

$SEED_FILE = "cassandra/seed_cameras.cql"
if (Test-Path $SEED_FILE) {
    Write-Host "seed file found ($SEED_FILE), copying to pod..." -ForegroundColor Cyan
    kubectl cp $SEED_FILE "$NAMESPACE/cassandra-client:/tmp/seed_cameras.cql"
    
    Write-Host "executing seed data insertion..." -ForegroundColor Cyan
    kubectl exec -n $NAMESPACE cassandra-client -- `
        cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -f /tmp/seed_cameras.cql
    
    Write-Host "seed data inserted successfully." -ForegroundColor Green
} else {
    Write-Host "warning: seed file $SEED_FILE not found." -ForegroundColor Yellow
}

Write-Host "`ncassandra setup completed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "=== HUONG DAN KIEM TRA ===" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Xem danh sach cac bang trong keyspace traffic_data:" -ForegroundColor Cyan
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; DESCRIBE TABLES;`""
Write-Host ""
Write-Host "2. Xem so luong row trong tung bang:" -ForegroundColor Cyan
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT COUNT(*) FROM camera_info;`""
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT COUNT(*) FROM traffic_windowed_by_camera;`""
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT COUNT(*) FROM traffic_windowed_all;`""
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT COUNT(*) FROM traffic_hourly_by_camera;`""
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT COUNT(*) FROM traffic_daily_by_camera;`""
Write-Host ""
Write-Host "3. Xem du lieu mau tu bang camera_info:" -ForegroundColor Cyan
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT * FROM camera_info LIMIT 10;`""
Write-Host ""
Write-Host "4. Xem du lieu mau tu bang traffic_windowed_by_camera:" -ForegroundColor Cyan
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT * FROM traffic_windowed_by_camera LIMIT 10;`""
Write-Host ""
Write-Host "5. Xem du lieu mau tu bang traffic_windowed_all:" -ForegroundColor Cyan
Write-Host "   kubectl exec -n $NAMESPACE cassandra-client -- cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -e `"USE traffic_data; SELECT * FROM traffic_windowed_all LIMIT 10;`""
Write-Host ""
Write-Host "6. Xoa cassandra-client pod:" -ForegroundColor Cyan
Write-Host "   kubectl delete pod cassandra-client -n $NAMESPACE"
