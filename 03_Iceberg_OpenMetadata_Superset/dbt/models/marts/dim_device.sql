{{
  config(
    materialized='table',
    post_hook =[
      "GRANT SELECT ON {{ this }} TO analyst_limited",
      "GRANT SELECT ON {{ this }} TO analyst_full"
    ]
  )
}}

-- Gold layer: DIM_DEVICE
-- This dimension table is derived from the stg_device staging table.
-- It includes the following columns:
--   - DeviceKey: Primary key
--   - Brand: Device brand (not null)
--   - Model: Device model (not null)
--   - MinPower: Minimum power of the device
--   - InstallationDate: Date of installation (not null)
--   - ValidTo: Validity end date (default '9999-12-31')

SELECT
  row_number() OVER () AS DeviceKey, -- Primary key
  Brand,
  Model,
  MinPower,
  InstallationDate,
  COALESCE(ValidTo, toDate('9999-12-31')) AS ValidTo
FROM {{ ref('stg_device') }}
WHERE Brand IS NOT NULL AND Model IS NOT NULL