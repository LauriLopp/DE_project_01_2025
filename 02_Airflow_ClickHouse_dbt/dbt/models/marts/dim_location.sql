{{
  config(
    materialized='table'
  )
}}

-- Gold layer: DIM_LOCATION
-- This dimension table is derived from the stg_location staging table.
-- It includes the following columns:
--   - LocationKey: Primary key
--   - DeviceLocation: Location of the device (not null)
--   - ClosestWeatherStation: Closest weather station (not null)
--   - PricingRegion: Pricing region (not null)
--   - ValidFrom: Start date of validity (not null)
--   - ValidTo: End date of validity (default '9999-12-31')

SELECT
  row_number() OVER () AS LocationKey, -- Primary key
  DeviceLocation,
  ClosestWeatherStation,
  PricingRegion,
  ValidFrom,
  COALESCE(ValidTo, toDate('9999-12-31')) AS ValidTo
FROM {{ ref('stg_location') }}
WHERE DeviceLocation IS NOT NULL 
  AND ClosestWeatherStation IS NOT NULL 
  AND PricingRegion IS NOT NULL