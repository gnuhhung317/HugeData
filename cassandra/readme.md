# các bảng trong cassandra

dưới đây là mô tả chi tiết về các bảng được sử dụng để lưu trữ và quản lý dữ liệu giao thông trong dự án:

## 1. traffic_metrics
- **mục đích**: lưu trữ dữ liệu giao thông chi tiết và tức thời từ các camera giám sát.
- **chi tiết**: bảng này ghi lại số lượng xe cụ thể cho từng loại phương tiện tại mỗi thời điểm. dữ liệu được phân vùng theo `camera_id` và sắp xếp theo `timestamp` giảm dần.
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh của camera.
  - `timestamp` (timestamp): thời điểm ghi nhận dữ liệu.
  - `total_count` (int): tổng số lượng phương tiện.
  - `car_count` (int): số lượng xe con.
  - `truck_count` (int): số lượng xe tải.
  - `bus_count` (int): số lượng xe buýt.
  - `motorcycle_count` (int): số lượng xe máy.

## 2. traffic_hourly
- **mục đích**: lưu trữ thông tin giao thông đã được tổng hợp theo đơn vị giờ.
- **chi tiết**: cung cấp các chỉ số đã được tính toán sẵn để tối ưu hóa hiệu suất cho các biểu đồ và báo cáo xu hướng theo giờ.
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh của camera.
  - `hour` (timestamp): mốc thời gian bắt đầu của giờ tổng hợp.
  - `total_count` (int): tổng số lượng xe trong giờ.
  - `avg_count` (double): số lượng xe trung bình.
  - `max_count` (int): số lượng xe lớn nhất tại một thời điểm trong giờ.
  - `min_count` (int): số lượng xe nhỏ nhất tại một thời điểm trong giờ.

## 3. traffic_daily
- **mục đích**: lưu trữ các số liệu thống kê tổng hợp theo từng ngày.
- **chi tiết**: tập trung vào các chỉ số quan trọng trong ngày như tổng lượng xe và xác định giờ cao điểm (peak hour).
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh của camera.
  - `date` (date): ngày thống kê.
  - `total_vehicles` (int): tổng số lượng xe trong ngày.
  - `peak_hour` (timestamp): thời điểm (giờ) có lưu lượng xe cao nhất.
  - `peak_count` (int): số lượng xe tại thời điểm cao điểm.

## 4. traffic_stats_windowed
- **mục đích**: lưu trữ dữ liệu thống kê theo các cửa sổ thời gian cố định (30 phút hoặc 1 giờ).
- **chi tiết**: được thiết kế tối ưu cho việc vẽ 5 loại biểu đồ đường trong grafana cho từng camera hoặc toàn bộ hệ thống.
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh camera hoặc 'ALL'.
  - `window_type` (text): loại cửa sổ thời gian ('30m' hoặc '1h').
  - `window_start` (timestamp): thời điểm bắt đầu cửa sổ.
  - `car_count` (int): tổng số xe con trong cửa sổ.
  - `bus_count` (int): tổng số xe buýt trong cửa sổ.
  - `truck_count` (int): tổng số xe tải trong cửa sổ.
  - `motorcycle_count` (int): tổng số xe máy trong cửa sổ.
  - `total_count` (int): tổng cộng tất cả xe trong cửa sổ.

## 5. camera_info
- **mục đích**: lưu trữ thông tin danh mục và metadata của các camera.
- **chi tiết**: phục vụ việc tạo dropdown menu và hiển thị vị trí trên bản đồ.
- **cấu trúc các cột**:
  - `camera_id` (text): mã định danh duy nhất của camera.
  - `camera_name` (text): tên hiển thị của camera.
  - `lat` (double): vĩ độ (latitude).
  - `lon` (double): kinh độ (longitude).
