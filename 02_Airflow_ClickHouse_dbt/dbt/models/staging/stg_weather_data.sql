{{
	config(
		materialized='view'
	)
}}

-- Staging: clean weather data
-- Goals:
-- 1) English column names
-- 2) Convert UTC timestamp to local time (Europe/Tallinn) and expose a DateTime column named `timestamp`
-- 3) Cast numeric fields to floats and turn missing values into NULLs
-- 4) Aggregate to hourly grain to match other data!!
-- 5) Add surrogate key

WITH raw AS (
	SELECT
		toTimeZone(last_changed, 'Europe/Tallinn') AS timestamp_raw,
		temperature_C       		AS temperature_c,
		dew_point_C         		AS dew_point_c,
		humidity_percent    		AS humidity_perc,
		cloud_coverage_pct  		AS cloud_coverage_pct,
		uv_index            		AS uv_index,
		pressure_hPa * 0.750061683	AS air_pressure_mmhg,
		wind_bearing_deg    		AS wind_direction_deg,
		wind_gust_speed_kmh / 3.6	AS wind_gust_speed_ms,
		wind_speed_kmh / 3.6      	AS wind_speed_ms,
		condition_state            AS condition_state
	FROM {{ source('bronze_layer', 'bronze_weather_history') }}
),

aggregated AS (
    SELECT
        -- Truncate timestamp to the hour for aggregation
        toStartOfHour(timestamp_raw) AS hourly_timestamp,
        avg(temperature_c)       AS temperature_c,
        avg(dew_point_c)         AS dew_point_c,
        avg(humidity_perc)       AS humidity_perc,
        avg(cloud_coverage_pct)  AS cloud_coverage_pct,
        avg(uv_index)            AS uv_index,
        avg(air_pressure_mmhg)   AS air_pressure_mmhg,
        avg(wind_direction_deg)  AS wind_direction_deg,
        avg(wind_gust_speed_ms)	 AS wind_gust_speed_ms,
		avg(wind_speed_ms)		 AS wind_speed_ms,
        argMax(condition_state, timestamp_raw) AS condition_state
    FROM raw
    WHERE timestamp_raw IS NOT NULL
    GROUP BY toStartOfHour(timestamp_raw)
)

SELECT
	row_number() over (order by hourly_timestamp) as WeatherKey,
    hourly_timestamp as timestamp,
    temperature_c,
    dew_point_c,
    humidity_perc,
    cloud_coverage_pct,
    uv_index,
    air_pressure_mmhg,
    wind_direction_deg,
    wind_gust_speed_ms,
    wind_speed_ms,
    condition_state
FROM aggregated