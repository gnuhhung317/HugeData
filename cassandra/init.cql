-- =====================================================
-- KEYSPACE
-- =====================================================
CREATE KEYSPACE IF NOT EXISTS traffic_data
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE traffic_data;

-- =====================================================
-- CAMERA METADATA
-- Used for Grafana variables only
-- =====================================================
CREATE TABLE IF NOT EXISTS camera_info (
  camera_id text PRIMARY KEY,
  camera_name text,
  lat double,
  lon double
);

-- =====================================================
-- RAW TRAFFIC EVENTS (OPTIONAL)
-- Not queried by Grafana directly
-- =====================================================
CREATE TABLE IF NOT EXISTS traffic_raw (
  camera_id text,
  event_time timestamp,
  vehicle_type text,
  PRIMARY KEY ((camera_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- =====================================================
-- WINDOWED STATS PER CAMERA (30m, 1h)
-- Grafana: per-camera time series panels
-- =====================================================
CREATE TABLE IF NOT EXISTS traffic_windowed_by_camera (
  camera_id text,
  window_type text,            -- '30m', '1h'
  window_start timestamp,
  car_count int,
  bus_count int,
  truck_count int,
  motorcycle_count int,
  total_count int,
  PRIMARY KEY ((camera_id, window_type), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- =====================================================
-- WINDOWED STATS - ALL CAMERAS (PRE-AGGREGATED)
-- Grafana: "Total Vehicles - All Cameras"
-- =====================================================
CREATE TABLE IF NOT EXISTS traffic_windowed_all (
  window_type text,            -- '30m', '1h'
  window_start timestamp,
  total_count int,
  PRIMARY KEY ((window_type), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- =====================================================
-- HOURLY STATISTICS PER CAMERA
-- Grafana: hourly trend panels
-- =====================================================
CREATE TABLE IF NOT EXISTS traffic_hourly_by_camera (
  camera_id text,
  hour_start timestamp,
  total_count int,
  avg_count double,
  max_count int,
  min_count int,
  PRIMARY KEY ((camera_id), hour_start)
) WITH CLUSTERING ORDER BY (hour_start DESC);

-- =====================================================
-- DAILY STATISTICS PER CAMERA
-- Grafana: daily summary panels
-- =====================================================
CREATE TABLE IF NOT EXISTS traffic_daily_by_camera (
  camera_id text,
  date date,
  total_count int,
  peak_hour timestamp,
  peak_count int,
  PRIMARY KEY ((camera_id), date)
) WITH CLUSTERING ORDER BY (date DESC);

-- =====================================================
-- OPTIONAL: WINDOWED STATS BY VEHICLE TYPE
-- Use only if vehicle-type panels grow large
-- =====================================================
CREATE TABLE IF NOT EXISTS traffic_vehicle_type_windowed (
  camera_id text,
  window_type text,            -- '30m', '1h'
  vehicle_type text,           -- 'car', 'bus', 'truck', 'motorcycle'
  window_start timestamp,
  count int,
  PRIMARY KEY ((camera_id, window_type, vehicle_type), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
