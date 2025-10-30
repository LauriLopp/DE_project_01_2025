/*
============================================================
 Raw data ingestion for project CSVs (ClickHouse SQL)
 - Creates database raw_data
 - Creates one raw table per CSV
 - Loads data from CSVs using table function file(..., 'CSVWithNames')

 Notes:
 - All file paths use only the filename (e.g., 'iot_data.csv') for compatibility with ClickHouse container.
============================================================
*/

-- Create raw database (idempotent)
CREATE DATABASE IF NOT EXISTS raw_data;

-- 1) IOT sensor data (columns known: entity_id, state, last_changed)
DROP TABLE IF EXISTS raw_data.iot_andmed;
CREATE TABLE raw_data.iot_andmed
(
    entity_id String,
    state String,
    last_changed DateTime
)
ENGINE = MergeTree()
ORDER BY (last_changed, entity_id);

-- Insert rows from CSV (header row present)
INSERT INTO raw_data.iot_andmed
SELECT
    entity_id,
    state,
    parseDateTimeBestEffort(last_changed) AS last_changed
FROM file('/var/lib/clickhouse/user_files/iot_data.csv',
          'CSVWithNames',
          'entity_id String, state String, last_changed String');

-- 2) Meteorological data (schema inferred as String columns)
DROP TABLE IF EXISTS raw_data.meteo_physicum;
CREATE TABLE raw_data.meteo_physicum
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT *
FROM file('/var/lib/clickhouse/user_files/weather_data.csv',
          'CSVWithNames');

-- 3) Electricity pricing data (schema inferred as String columns)
DROP TABLE IF EXISTS raw_data.np_tunnihinnad;
CREATE TABLE raw_data.np_tunnihinnad
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT *
FROM file('/var/lib/clickhouse/user_files/price_data.csv',
          'CSVWithNames');

/*
 Additional raw tables for device and location sample CSVs.
 Prefer reading from user_files mount to work inside the container.
*/

-- 4) Device model info
DROP TABLE IF EXISTS raw_data.device_data;
CREATE TABLE raw_data.device_data
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT *
FROM file('/var/lib/clickhouse/user_files/device_data.csv', 'CSVWithNames');

-- 5) Device location SCD history
DROP TABLE IF EXISTS raw_data.location_data;
CREATE TABLE raw_data.location_data
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT *
FROM file('/var/lib/clickhouse/user_files/location_data.csv', 'CSVWithNames');