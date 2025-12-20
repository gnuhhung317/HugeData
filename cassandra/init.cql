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
