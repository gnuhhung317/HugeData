-- create keyspace for traffic data
CREATE KEYSPACE IF NOT EXISTS traffic_data 
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- use the keyspace
USE traffic_data;

-- create table for traffic metrics
CREATE TABLE IF NOT EXISTS traffic_metrics (
  camera_id text,
  timestamp timestamp,
  total_count int,
  car_count int,
  truck_count int,
  bus_count int,
  motorcycle_count int,
  PRIMARY KEY ((camera_id), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- create table for aggregated hourly data
CREATE TABLE IF NOT EXISTS traffic_hourly (
  camera_id text,
  hour timestamp,
  total_count int,
  avg_count double,
  max_count int,
  min_count int,
  PRIMARY KEY ((camera_id), hour)
) WITH CLUSTERING ORDER BY (hour DESC);

-- create table for daily statistics
CREATE TABLE IF NOT EXISTS traffic_daily (
  camera_id text,
  date date,
  total_vehicles int,
  peak_hour timestamp,
  peak_count int,
  PRIMARY KEY ((camera_id), date)
) WITH CLUSTERING ORDER BY (date DESC);

-- create table for windowed statisics (30m, 1h)
CREATE TABLE IF NOT EXISTS traffic_stats_windowed (
  camera_id text,
  window_type text,
  window_start timestamp,
  car_count int,
  bus_count int,
  truck_count int,
  motorcycle_count int,
  total_count int,
  PRIMARY KEY ((camera_id, window_type), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- create table for camera metadata
CREATE TABLE IF NOT EXISTS camera_info (
  camera_id text PRIMARY KEY,
  camera_name text,
  lat double,
  lon double
);
