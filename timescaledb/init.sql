-- Create TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create traffic_metrics table
CREATE TABLE IF NOT EXISTS traffic_metrics (
    time TIMESTAMPTZ NOT NULL,
    camera_id VARCHAR(50) NOT NULL,
    camera_name VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    car_count INTEGER DEFAULT 0,
    motorcycle_count INTEGER DEFAULT 0,
    bus_count INTEGER DEFAULT 0,
    truck_count INTEGER DEFAULT 0,
    total_count INTEGER DEFAULT 0,
    PRIMARY KEY (time, camera_id)
);

-- Convert to hypertable
SELECT create_hypertable('traffic_metrics', 'time', if_not_exists => TRUE);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_camera_id ON traffic_metrics (camera_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_time ON traffic_metrics (time DESC);

-- Create continuous aggregate for hourly metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_metrics_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', time) AS bucket,
    camera_id,
    camera_name,
    AVG(latitude) AS latitude,
    AVG(longitude) AS longitude,
    AVG(car_count) AS avg_car_count,
    AVG(motorcycle_count) AS avg_motorcycle_count,
    AVG(bus_count) AS avg_bus_count,
    AVG(truck_count) AS avg_truck_count,
    AVG(total_count) AS avg_total_count,
    MAX(total_count) AS max_total_count,
    MIN(total_count) AS min_total_count
FROM traffic_metrics
GROUP BY bucket, camera_id, camera_name;

-- Add refresh policy for continuous aggregate (refresh every hour)
SELECT add_continuous_aggregate_policy('traffic_metrics_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- Create retention policy (keep data for 30 days)
SELECT add_retention_policy('traffic_metrics', INTERVAL '30 days', if_not_exists => TRUE);

-- Create view for latest metrics per camera
CREATE OR REPLACE VIEW latest_traffic_metrics AS
SELECT DISTINCT ON (camera_id)
    time,
    camera_id,
    camera_name,
    latitude,
    longitude,
    car_count,
    motorcycle_count,
    bus_count,
    truck_count,
    total_count
FROM traffic_metrics
ORDER BY camera_id, time DESC;
