{{
	config(
		materialized='view'
	)
}}

-- Staging: clean weather data (local time already)
-- Goals:
-- 1) English column names
-- 2) Keep local time and expose a DateTime('Europe/Tallinn') column named `timestamp`
-- 3) Cast numeric fields to floats and turn missing values into NULLs
-- 4) Aggregate to hourly grain to match other data!!
-- 5) Add surrogate key


WITH raw AS (
	SELECT
		`Aeg`                                  AS time_raw,
		`Temperatuur`                          AS temperature_raw,
		`Niiskus`                              AS humidity_raw,
		`&Otilde;hur&otilde;hk`                AS air_pressure_raw,
		`Tuule kiirus`                         AS wind_speed_raw,
		`Tuule suund`                          AS wind_dir_raw,
		`Sademed`                              AS precipitation_raw,
		`UV indeks`                            AS uv_index_raw,
		`Valgustatus`                          AS illuminance_raw,
		`Kiirgusvoog`                          AS irradiance_raw
	FROM {{ source('raw_data', 'meteo_physicum') }}
), typed AS (
	SELECT
		-- Interpret input as local time and keep type in local timezone
		CAST(parseDateTimeBestEffortOrNull(time_raw, 'Europe/Tallinn') AS DateTime('Europe/Tallinn')) AS timestamp,
		toFloat64OrNull(temperature_raw)    AS temperature_c,
		toFloat64OrNull(humidity_raw)       AS humidity_perc,
		toFloat64OrNull(air_pressure_raw)   AS air_pressure_hpa,
		toFloat64OrNull(wind_speed_raw)     AS wind_speed_ms,
		toFloat64OrNull(wind_dir_raw)       AS wind_direction_deg,
		toFloat64OrNull(precipitation_raw)  AS precipitation_mm,
		toFloat64OrNull(uv_index_raw)       AS uv_index,
		toFloat64OrNull(illuminance_raw)    AS illuminance_lux,
		toFloat64OrNull(irradiance_raw)     AS solar_irradiance
	FROM raw
)
SELECT
	row_number() over () as WeatherKey,
	-- Truncate timestamp to the hour for aggregation
	toStartOfHour(timestamp) AS timestamp,
	avg(temperature_c)       AS temperature_c,
	avg(humidity_perc)       AS humidity_perc,
	avg(air_pressure_hpa)    AS air_pressure_hpa,
	avg(wind_speed_ms)       AS wind_speed_ms,
	avg(wind_direction_deg)  AS wind_direction_deg,
	sum(precipitation_mm)    AS precipitation_mm,
	avg(uv_index)            AS uv_index,
	avg(illuminance_lux)     AS illuminance_lux,
	avg(solar_irradiance)    AS solar_irradiance
FROM typed
WHERE timestamp IS NOT NULL
GROUP BY toStartOfHour(timestamp)
