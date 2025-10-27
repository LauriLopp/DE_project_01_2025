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
	timestamp,
	temperature_c,
	humidity_perc,
	air_pressure_hpa,
	wind_speed_ms,
	wind_direction_deg,
	precipitation_mm,
	uv_index,
	illuminance_lux,
	solar_irradiance
FROM typed
WHERE timestamp IS NOT NULL
