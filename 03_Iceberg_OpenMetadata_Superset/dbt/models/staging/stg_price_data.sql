{{
	config(
		materialized='view'
	)
}}

-- Converts EUR/MWh to EUR/kWh, aggregates to hourly

WITH raw AS (
	SELECT
		toTimeZone(ts_utc, 'Europe/Tallinn') AS timestamp_raw,
		price_per_mwh AS price_eur_per_mwh
	FROM {{ source('bronze_layer', 'bronze_elering_iceberg_readonly') }}
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
	avg_price_eur_per_mwh / 1000.0 AS price_eur_per_kwh
FROM aggregated
WHERE timestamp IS NOT NULL
