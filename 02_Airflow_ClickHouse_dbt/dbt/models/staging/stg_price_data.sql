{{
	config(
		materialized='view'
	)
}}

-- Staging: clean electricity price data
-- Requirements:
-- 1) Meaningful column names
-- 2) Consistently use local time (Europe/Tallinn) - already in bronze layer
-- 3) Consistently use a DateTime column named `timestamp`
-- 4) Numeric values as floats, convert EUR/MWh -> EUR/kWh
-- 5) Aggregate to hourly averages if granularity is higher than hour

WITH raw AS (
	SELECT
		toTimeZone(ts_utc, 'Europe/Tallinn') AS timestamp_raw,
		price_per_mwh AS price_eur_per_mwh
	FROM {{ source('bronze_iot_raw_data', 'bronze_elering_price') }}
), aggregated AS (
	SELECT
		toStartOfHour(timestamp_raw) AS timestamp,
		avg(price_eur_per_mwh) AS avg_price_eur_per_mwh
	FROM raw
	GROUP BY toStartOfHour(timestamp_raw)
)
SELECT
	row_number() over () as PriceKey,
	timestamp,
	/* Convert EUR/MWh -> EUR/kWh */
	avg_price_eur_per_mwh / 1000.0 AS price_eur_per_kwh,
FROM aggregated
WHERE timestamp IS NOT NULL
