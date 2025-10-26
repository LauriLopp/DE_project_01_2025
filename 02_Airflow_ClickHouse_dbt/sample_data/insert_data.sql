/*
============================================================
 Raw data ingestion for project CSVs (ClickHouse SQL)
 - Creates database raw_data
 - Creates one raw table per CSV
 - Loads data from CSVs using table function file(..., 'CSVWithNames')

 Notes:
 - Paths below use absolute Windows paths for clarity. Adjust to your ClickHouse server's accessible path (e.g., user_files).
 - For meteo and price CSVs, we infer all columns as String via CREATE TABLE AS SELECT; refine types later in dbt models.
============================================================
*/

-- Create raw database (idempotent)
CREATE DATABASE IF NOT EXISTS raw_data;

-- 1) IOT sensor data (columns known: entity_id, state, last_changed)
DROP TABLE IF EXISTS raw_data.iot_andmed;
CREATE TABLE raw_data.iot_andmed
(
    entity_id    String,
    state        String,
    last_changed DateTime
)
ENGINE = MergeTree
ORDER BY (last_changed, entity_id);

-- Insert rows from CSV (header row present)
INSERT INTO raw_data.iot_andmed (entity_id, state, last_changed)
SELECT
    entity_id,
    state,
    parseDateTimeBestEffort(last_changed) AS last_changed
FROM file('c:/Users/loppl/Documents_C/MAKA/DE_aka_Andmetehnika/DE_project_2025/IOT_andmed_7.12.24-17.03.25.csv',
          'CSVWithNames',
          'entity_id String, state String, last_changed String');

-- 2) Meteorological data (schema inferred as String columns)
DROP TABLE IF EXISTS raw_data.meteo_physicum;
CREATE TABLE raw_data.meteo_physicum
ENGINE = MergeTree
ORDER BY tuple()
AS
SELECT *
FROM file('c:/Users/loppl/Documents_C/MAKA/DE_aka_Andmetehnika/DE_project_2025/Meteo Physicum archive 071224-170325 - archive.csv',
          'CSVWithNames');

-- 3) Electricity pricing data (schema inferred as String columns)
DROP TABLE IF EXISTS raw_data.np_tunnihinnad;
CREATE TABLE raw_data.np_tunnihinnad
ENGINE = MergeTree
ORDER BY tuple()
AS
SELECT *
FROM file('c:/Users/loppl/Documents_C/MAKA/DE_aka_Andmetehnika/DE_project_2025/NP tunnihinnad    071224-170325.csv',
          'CSVWithNames');