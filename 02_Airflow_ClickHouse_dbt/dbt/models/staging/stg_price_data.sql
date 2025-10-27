{{
	config(
		materialized='view'
	)
}}

-- Staging: clean electricity price data
-- Requirements:
-- 1) Meaningful column names
-- 2) Consistently use local time (Europe/Tallinn)
-- 3) Consistently use a DateTime column named `timestamp`
-- 4) Numeric values as floats (decimal comma -> dot), convert cents/kWh -> EUR/kWh

WITH raw AS (
	SELECT
		/* Original headers are semicolon-separated CSV with localized names */
		`Periood` AS period_raw,
		`Börsihind senti/kWh (sisaldab käibemaksu)` AS price_with_vat_cents_raw,
		`Börsihind senti/kWh (ei sisalda käibemaksu)` AS price_without_vat_cents_raw
	FROM {{ source('raw_data', 'np_tunnihinnad') }}
), typed AS (
	SELECT
		/* Parse dd.MM.yyyy HH:mm as local time (no shift) and keep type as DateTime('Europe/Tallinn') */
		CAST(parseDateTimeBestEffortOrNull(period_raw, 'Europe/Tallinn') AS DateTime('Europe/Tallinn')) AS timestamp,
		/* Convert decimal comma to dot and cast to float (values are cents/kWh) */
		toFloat64OrNull(replaceAll(price_with_vat_cents_raw, ',', '.'))      AS price_with_vat_cents,
		toFloat64OrNull(replaceAll(price_without_vat_cents_raw, ',', '.'))   AS price_without_vat_cents
	FROM raw
)
SELECT
	row_number() over () as PriceKey,
	timestamp,
	/* Convert cents -> EUR per kWh */
	price_with_vat_cents / 100.0       AS price_eur_with_vat,
	price_without_vat_cents / 100.0    AS price_eur_without_vat
FROM typed
WHERE timestamp IS NOT NULL
