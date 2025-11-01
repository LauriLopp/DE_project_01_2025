{{ config(materialized='table') }}

SELECT
  row_number() OVER () AS LocationKey,
  DeviceLocation,
  ClosestWeatherStation,
  PricingRegion,
  COALESCE(ValidFrom, toDate('2000-01-01')) AS ValidFrom,
  COALESCE(ValidTo,   toDate('9999-12-31')) AS ValidTo
FROM {{ ref('stg_location') }}
WHERE DeviceLocation IS NOT NULL
  AND ClosestWeatherStation IS NOT NULL
  AND PricingRegion IS NOT NULL
