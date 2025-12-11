# Hướng dẫn Deploy Spark Streaming → TimescaleDB → Grafana

## Tổng quan
Pipeline này thu thập dữ liệu từ Kafka, xử lý qua Spark Streaming, lưu vào TimescaleDB (time-series database) và trực quan hóa bằng Grafana.

```
Kafka Producer → Spark Streaming → TimescaleDB → Grafana Dashboard
```

## Kiến trúc

- **Kafka**: Message queue chứa traffic events
- **Spark Streaming**: Xử lý real-time data từ Kafka
- **TimescaleDB**: PostgreSQL extension tối ưu cho time-series data
- **Grafana**: Visualization và monitoring dashboard

## Các bước triển khai

### 1. Deploy TimescaleDB

```bash
kubectl apply -f k8s/timescaledb.yaml
```

Đợi pod TimescaleDB sẵn sàng:
```bash
kubectl wait --for=condition=ready pod -l app=timescaledb -n hugedata --timeout=300s
```

### 2. Khởi tạo Database Schema

Port-forward TimescaleDB để kết nối:
```bash
kubectl port-forward svc/timescaledb 5432:5432 -n hugedata
```

Chạy script khởi tạo:
```bash
# Từ terminal khác
psql -h localhost -U postgres -d traffic -f timescaledb/init.sql
# Password: postgres
```

Hoặc exec vào pod:
```bash
kubectl exec -it timescaledb-0 -n hugedata -- psql -U postgres -d traffic
```

Paste nội dung từ `timescaledb/init.sql` hoặc:
```bash
kubectl exec -it timescaledb-0 -n hugedata -- bash -c "psql -U postgres -d traffic" < timescaledb/init.sql
```

### 3. Build lại Spark Image với PostgreSQL Driver

```bash
cd spark
docker build -t spark-application:dev .
```

Nếu dùng Minikube:
```bash
minikube image load spark-application:dev
```

Nếu dùng Kind:
```bash
kind load docker-image spark-application:dev
```

### 4. Deploy Spark Application

```bash
kubectl apply -f k8s/spark-app.yaml
```

Kiểm tra logs:
```bash
kubectl logs -f spark-pi-python-driver -n hugedata
```

### 5. Deploy Grafana

```bash
kubectl apply -f k8s/grafana.yaml
```

Đợi Grafana sẵn sàng:
```bash
kubectl wait --for=condition=ready pod -l app=grafana -n hugedata --timeout=300s
```

### 6. Truy cập Grafana

Port-forward Grafana:
```bash
kubectl port-forward svc/grafana 3000:3000 -n hugedata
```

Mở browser: http://localhost:3000
- Username: `admin`
- Password: `admin`

Dashboard "Traffic Monitoring Dashboard" đã được tự động cấu hình.

## Kiểm tra Pipeline

### Kiểm tra data trong TimescaleDB

```bash
kubectl port-forward svc/timescaledb 5432:5432 -n hugedata
psql -h localhost -U postgres -d traffic
```

Các queries hữu ích:
```sql
-- Xem số lượng records
SELECT COUNT(*) FROM traffic_metrics;

-- Xem latest records
SELECT * FROM traffic_metrics ORDER BY time DESC LIMIT 10;

-- Xem metrics theo camera
SELECT 
  camera_id,
  camera_name,
  COUNT(*) as records,
  AVG(total_count) as avg_vehicles
FROM traffic_metrics
WHERE time > NOW() - INTERVAL '1 hour'
GROUP BY camera_id, camera_name
ORDER BY avg_vehicles DESC;

-- Xem continuous aggregate (hourly)
SELECT * FROM traffic_metrics_hourly ORDER BY bucket DESC LIMIT 10;
```

### Kiểm tra Spark Streaming

```bash
# Logs của Spark driver
kubectl logs -f spark-pi-python-driver -n hugedata

# Logs của Spark executor
kubectl logs -f spark-pi-python-exec-1 -n hugedata
```

## Grafana Dashboard Features

Dashboard "Traffic Monitoring" bao gồm:

1. **Total Vehicle Count Over Time**: Line chart tổng số xe theo thời gian
2. **Vehicle Types Distribution**: Stacked area chart phân loại xe
3. **Top 10 Busiest Cameras**: Bar gauge camera có traffic cao nhất
4. **Latest Traffic Metrics**: Table hiển thị metrics mới nhất
5. **Statistics Cards**: 
   - Total Vehicles Detected
   - Active Cameras
   - Avg Vehicles per Camera
   - Vehicle Type Pie Chart

Auto-refresh mỗi 5 giây.

## Cấu hình nâng cao

### Tăng retention policy

Mặc định data được giữ 30 ngày. Để thay đổi:

```sql
SELECT remove_retention_policy('traffic_metrics');
SELECT add_retention_policy('traffic_metrics', INTERVAL '90 days');
```

### Thêm continuous aggregates

Ví dụ tạo daily aggregate:

```sql
CREATE MATERIALIZED VIEW traffic_metrics_daily
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', time) AS bucket,
    camera_id,
    AVG(total_count) AS avg_total_count,
    MAX(total_count) AS max_total_count
FROM traffic_metrics
GROUP BY bucket, camera_id;

SELECT add_continuous_aggregate_policy('traffic_metrics_daily',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');
```

### Tăng resources cho Spark

Chỉnh trong `k8s/spark-app.yaml`:

```yaml
driver:
  cores: 2
  memory: 2g
executor:
  instances: 2
  cores: 2
  memory: 2g
```

### Alert trong Grafana

Có thể thêm alert rules trong Grafana để cảnh báo khi:
- Traffic vượt ngưỡng
- Không nhận được data từ camera
- Spike đột ngột về số lượng xe

## Troubleshooting

### Spark không ghi được vào TimescaleDB

1. Kiểm tra kết nối:
```bash
kubectl exec -it spark-pi-python-driver -n hugedata -- nc -zv timescaledb.hugedata.svc.cluster.local 5432
```

2. Kiểm tra PostgreSQL JDBC driver đã được load:
```bash
kubectl exec -it spark-pi-python-driver -n hugedata -- ls -la /opt/spark/jars/ | grep postgresql
```

3. Xem logs chi tiết:
```bash
kubectl logs spark-pi-python-driver -n hugedata | grep -i timescale
```

### Grafana không hiển thị data

1. Test datasource trong Grafana UI: Configuration > Data Sources > TimescaleDB > Test
2. Kiểm tra query trực tiếp trong TimescaleDB
3. Xem Grafana logs:
```bash
kubectl logs -f deployment/grafana -n hugedata
```

### Performance issues

1. Tăng checkpoint interval trong Spark
2. Batch writes lớn hơn vào TimescaleDB
3. Thêm indexes trong TimescaleDB
4. Tăng resources cho pods

## Cleanup

```bash
kubectl delete -f k8s/grafana.yaml
kubectl delete -f k8s/spark-app.yaml
kubectl delete -f k8s/timescaledb.yaml
```

## Tài liệu tham khảo

- [TimescaleDB Documentation](https://docs.timescale.com/)
- [Grafana Provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/)
- [Spark Structured Streaming + Kafka](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/)
