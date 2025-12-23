## 1. camera_info
- **mục đích**: lưu trữ thông tin danh mục và metadata của các camera.
- **chi tiết**: phục vụ việc tạo dropdown menu trong grafana và hiển thị vị trí trên bản đồ.
- **partition key**: `camera_id`
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh duy nhất của camera.
  - `camera_name` (text): tên hiển thị của camera.
  - `lat` (double): vĩ độ (latitude).
  - `lon` (double): kinh độ (longitude).

## 2. traffic_windowed_by_camera
- **mục đích**: lưu trữ dữ liệu thống kê theo các cửa sổ thời gian cố định (30 phút hoặc 1 giờ) cho từng camera.
- **chi tiết**: được thiết kế tối ưu cho việc vẽ biểu đồ đường trong grafana theo từng camera và loại phương tiện.
- **partition key**: `(camera_id, window_type)` - **BẮT BUỘC** phải có cả 2 trong WHERE clause
- **clustering key**: `window_start DESC` (mới nhất trước)
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh camera.
  - `window_type` (text): loại cửa sổ thời gian ('30m' hoặc '1h').
  - `window_start` (timestamp): thời điểm bắt đầu cửa sổ.
  - `car_count` (int): tổng số xe con trong cửa sổ.
  - `bus_count` (int): tổng số xe buýt trong cửa sổ.
  - `truck_count` (int): tổng số xe tải trong cửa sổ.
  - `motorcycle_count` (int): tổng số xe máy trong cửa sổ.
  - `total_count` (int): tổng cộng tất cả xe trong cửa sổ.
- **grafana query**: `WHERE camera_id = '$camera_id' AND window_type = '$window_type'`
- **lưu ý**: query grafana PHẢI bao gồm cả `camera_id` và `window_type` trong WHERE để tránh ALLOW FILTERING.

## 3. traffic_windowed_all
- **mục đích**: lưu trữ dữ liệu tổng hợp của TẤT CẢ camera theo cửa sổ thời gian.
- **chi tiết**: dữ liệu đã được tổng hợp sẵn từ tất cả camera, phục vụ biểu đồ "Total Vehicles (All Cameras)".
- **partition key**: `window_type` ('30m' hoặc '1h')
- **clustering key**: `window_start DESC` (mới nhất trước)
- **cấu trúc các cột**:
  - `window_type` (text): loại cửa sổ thời gian ('30m' hoặc '1h').
  - `window_start` (timestamp): thời điểm bắt đầu cửa sổ.
  - `total_count` (int): tổng số xe từ tất cả camera.
- **grafana query**: `WHERE window_type = '$window_type'`
- **lưu ý**: partition key là `window_type` để tránh ALLOW FILTERING. mỗi window_type là một partition riêng.

## 4. traffic_hourly_by_camera
- **mục đích**: lưu trữ thông tin giao thông đã được tổng hợp theo đơn vị giờ cho từng camera.
- **chi tiết**: cung cấp các chỉ số đã được tính toán sẵn để tối ưu hóa hiệu suất cho các biểu đồ và báo cáo xu hướng theo giờ.
- **partition key**: `camera_id`
- **clustering key**: `hour_start DESC` (mới nhất trước)
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh của camera.
  - `hour_start` (timestamp): mốc thời gian bắt đầu của giờ tổng hợp.
  - `total_count` (int): tổng số lượng xe trong giờ.
  - `avg_count` (double): số lượng xe trung bình.
  - `max_count` (int): số lượng xe lớn nhất tại một thời điểm trong giờ.
  - `min_count` (int): số lượng xe nhỏ nhất tại một thời điểm trong giờ.
- **grafana query**: `WHERE camera_id = '$camera_id'`

## 5. traffic_daily_by_camera
- **mục đích**: lưu trữ các số liệu thống kê tổng hợp theo từng ngày cho từng camera.
- **chi tiết**: tập trung vào các chỉ số quan trọng trong ngày như tổng lượng xe và xác định giờ cao điểm (peak hour).
- **partition key**: `camera_id`
- **clustering key**: `date DESC` (mới nhất trước)
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh của camera.
  - `date` (date): ngày thống kê.
  - `total_count` (int): tổng số lượng xe trong ngày.
  - `peak_hour` (timestamp): thời điểm (giờ) có lưu lượng xe cao nhất.
  - `peak_count` (int): số lượng xe tại thời điểm cao điểm.
- **grafana query**: `WHERE camera_id = '$camera_id'`

## 6. traffic_raw (optional)
- **mục đích**: lưu trữ dữ liệu giao thông thô từ các camera (không dùng cho grafana).
- **chi tiết**: bảng này chỉ dùng để lưu trữ dữ liệu gốc, phục vụ việc reprocessing hoặc audit. grafana KHÔNG query trực tiếp từ bảng này.
- **partition key**: `camera_id`
- **clustering key**: `event_time DESC` (mới nhất trước)
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh camera.
  - `event_time` (timestamp): thời điểm ghi nhận sự kiện.
  - `vehicle_type` (text): loại phương tiện ('car', 'bus', 'truck', 'motorcycle').

## 7. traffic_vehicle_type_windowed (optional)
- **mục đích**: lưu trữ dữ liệu windowed theo từng loại phương tiện riêng biệt.
- **chi tiết**: chỉ sử dụng nếu cần tách biệt dữ liệu theo vehicle type để tối ưu query performance.
- **partition key**: `(camera_id, window_type, vehicle_type)`
- **clustering key**: `window_start DESC` (mới nhất trước)
- **cấu trúc các cột**
  - `camera_id` (text): mã định danh camera.
  - `window_type` (text): loại cửa sổ thời gian ('30m' hoặc '1h').
  - `vehicle_type` (text): loại phương tiện ('car', 'bus', 'truck', 'motorcycle').
  - `window_start` (timestamp): thời điểm bắt đầu cửa sổ.
  - `count` (int): số lượng phương tiện loại này trong cửa sổ.
- **grafana query**: `WHERE camera_id = '$camera_id' AND window_type = '$window_type' AND vehicle_type = 'car'`
- **lưu ý**: chỉ dùng khi cần tối ưu cao, thường thì dùng `traffic_windowed_by_camera` là đủ.
