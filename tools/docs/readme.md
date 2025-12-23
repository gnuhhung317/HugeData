Sau khi đã chạy xong các lệnh ở k8s/docs/readme.md, ta sẽ chọn để chạy ứng với 1 file hoặc toàn bộ file từ hdfs như dưới đây.

# Với 1 file parquet 

Giả sử file parquet cần tải về từ HDFS là: part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.parquet
Namenode pod của bạn là: hdfs-namenode-0. Bạn chạy 2 lệnh này:
## Tải file parquet từ hdfs về máy

### 1) Kéo file từ HDFS ra /tmp trong pod
```bash
kubectl exec -n hugedata hdfs-namenode-0 -- hdfs dfs -get ^
/traffic_data/traffic_stream/part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.parquet ^
/tmp/part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.parquet
```
### 2) Copy từ pod về máy (về thư mục bạn đang đứng D:\HUST_22-26\PROJECT\HugeData)

```bash
kubectl cp -n hugedata hdfs-namenode-0:/tmp/part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.parquet ^
.\part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.parquet
```

(Tuỳ chọn) Xoá file tạm trong pod:
```bash
kubectl exec -n hugedata hdfs-namenode-0 -- rm -f /tmp/part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.parquet
```

## Tool 1 — Convert Parquet → CSV (lưu vĩnh viễn trên máy)
### 1) Cài thư viện (1 lần)
```bash
pip install pandas pyarrow
```
### 2) Chạy convert (Windows CMD / VSCode Terminal)

Lưu ý: với CMD, nếu muốn viết nhiều dòng thì dùng ký tự ^

```bash
cd /d D:\HUST_22-26\PROJECT\HugeData

python tools\parquet_to_csv.py ^
  --input "D:\HUST_22-26\PROJECT\HugeData\part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.parquet" ^
  --output "D:\HUST_22-26\PROJECT\HugeData\part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.csv"
```

## Tool 2 — Upload CSV lên HDFS (không cần cài hdfs trên Windows)

```bash
cd /d D:\HUST_22-26\PROJECT\HugeData

set NS=hugedata
set POD=hdfs-namenode-0
set HDFS_DIR=/traffic_data/csv

set FILE=part-00000-00eb2533-c860-44e1-93d1-837652fb905e-c000.snappy.csv

# REM 1) tạo thư mục trên HDFS (nếu chưa có)
kubectl exec -n %NS% %POD% -- hdfs dfs -mkdir -p %HDFS_DIR%

#REM 2) copy CSV từ máy -> pod (dùng đường dẫn tương đối để tránh lỗi D:)
kubectl cp ".\%FILE%" -n %NS% %POD%:/tmp/%FILE%

#REM 3) put từ pod -> HDFS
kubectl exec -n %NS% %POD% -- hdfs dfs -put -f /tmp/%FILE% %HDFS_DIR%/

#REM 4) kiểm tra file đã lên HDFS
kubectl exec -n %NS% %POD% -- hdfs dfs -ls %HDFS_DIR%

```

Sau đó bạn có thể kiểm tra trên UI: http://localhost:9870 → Browse → /traffic_data/csv.


# Chuyển toàn bộ file parquet ở hdfs thành csv, gộp lại thành 1 file csv duy nhất rồi upload lên hdfs
Với các giá trị, thay đổi trong file yaml nếu 3 giá trị trên không khớp với máy.
- serviceAccount: default
- image: gnuhhung317/spark-realtime-app
- sparkVersion: 3.5.1

## 1) Tạo ConfigMap chứa script

```bash
kubectl create configmap parquet-to-one-csv-script -n hugedata ^
  --from-file=hdfs_parquet_dir_to_one_csv_spark.py=tools\hdfs_parquet_dir_to_one_csv_spark.py ^
  --dry-run=client -o yaml | kubectl apply -f -

```

```bash
kubectl apply -f tools\spark_parquet_to_one_csv.yaml
```

## 2) Theo dõi chạy và xem log

```bash
kubectl get sparkapplications -n hugedata
kubectl get pods -n hugedata | findstr parquet-to-one-csv
kubectl logs -n hugedata -f parquet-to-one-csv-driver
```
Khi log có dòng kiểu [OK] Wrote: hdfs:///traffic_data/csv/traffic_all.csv là xong.

## 3) Kiểm tra file CSV đã nằm trên HDFS chưa

```bash
set NS=hugedata
set POD=hdfs-namenode-0

kubectl exec -n %NS% %POD% -- hdfs dfs -ls /traffic_data/csv
kubectl exec -n %NS% %POD% -- hdfs dfs -du -h /traffic_data/csv/traffic_all.csv
```

Rồi vào UI http://localhost:9870 → Browse → /traffic_data/csv sẽ thấy traffic_all.csv.


