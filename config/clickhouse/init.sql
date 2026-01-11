-- ClickHouse Initialization Script
-- Creates database and tables for airline delay data

-- Create database
CREATE DATABASE IF NOT EXISTS airline_data;

USE airline_data;

-- Main flights table with all delay information
CREATE TABLE IF NOT EXISTS flights (
    id String,
    year UInt16,
    month UInt8,
    carrier String,
    carrier_name String,
    airport String,
    airport_name String,
    arr_flights UInt32,
    arr_del15 UInt32,
    carrier_ct Float32,
    weather_ct Float32,
    nas_ct Float32,
    security_ct Float32,
    late_aircraft_ct Float32,
    arr_cancelled UInt32,
    arr_diverted UInt32,
    arr_delay UInt32,
    carrier_delay UInt32,
    weather_delay UInt32,
    nas_delay UInt32,
    security_delay UInt32,
    late_aircraft_delay UInt32,
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (year, month, carrier, airport)
PARTITION BY toYYYYMM(toDate(concat(toString(year), '-', toString(month), '-01')))
SETTINGS index_granularity = 8192;

-- Carriers lookup table (normalized)
CREATE TABLE IF NOT EXISTS carriers (
    carrier_code String,
    carrier_name String,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY carrier_code;

-- Airports lookup table (normalized)
CREATE TABLE IF NOT EXISTS airports (
    airport_code String,
    airport_name String,
    city String,
    state String,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY airport_code;

-- Materialized view for daily aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_delay_summary
ENGINE = SummingMergeTree()
ORDER BY (year, month, carrier, airport)
AS SELECT
    year,
    month,
    carrier,
    airport,
    sum(arr_flights) as total_flights,
    sum(arr_del15) as total_delayed,
    sum(arr_cancelled) as total_cancelled,
    sum(arr_diverted) as total_diverted,
    sum(arr_delay) as total_delay_minutes,
    sum(carrier_delay) as total_carrier_delay,
    sum(weather_delay) as total_weather_delay,
    sum(nas_delay) as total_nas_delay,
    sum(security_delay) as total_security_delay,
    sum(late_aircraft_delay) as total_late_aircraft_delay
FROM flights
GROUP BY year, month, carrier, airport;

-- Materialized view for carrier performance
CREATE MATERIALIZED VIEW IF NOT EXISTS carrier_performance
ENGINE = SummingMergeTree()
ORDER BY (year, month, carrier)
AS SELECT
    year,
    month,
    carrier,
    carrier_name,
    sum(arr_flights) as total_flights,
    sum(arr_del15) as delayed_flights,
    round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_percentage,
    round(sum(arr_delay) / sum(arr_flights), 2) as avg_delay_minutes
FROM flights
GROUP BY year, month, carrier, carrier_name;

-- Materialized view for airport performance
CREATE MATERIALIZED VIEW IF NOT EXISTS airport_performance
ENGINE = SummingMergeTree()
ORDER BY (year, month, airport)
AS SELECT
    year,
    month,
    airport,
    airport_name,
    sum(arr_flights) as total_flights,
    sum(arr_del15) as delayed_flights,
    round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_percentage,
    round(sum(arr_delay) / sum(arr_flights), 2) as avg_delay_minutes
FROM flights
GROUP BY year, month, airport, airport_name;

-- Real-time monitoring view (last hour)
CREATE MATERIALIZED VIEW IF NOT EXISTS realtime_stats
ENGINE = AggregatingMergeTree()
ORDER BY (ingestion_minute)
AS SELECT
    toStartOfMinute(ingestion_timestamp) as ingestion_minute,
    countState() as record_count,
    sumState(arr_flights) as total_flights,
    sumState(arr_del15) as total_delayed,
    avgState(toFloat32(arr_delay) / toFloat32(arr_flights)) as avg_delay_rate
FROM flights
GROUP BY ingestion_minute;
