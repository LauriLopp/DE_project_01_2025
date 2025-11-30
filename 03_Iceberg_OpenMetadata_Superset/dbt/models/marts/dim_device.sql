{{
  config(
    materialized='table',
    post_hook =[
      "GRANT SELECT ON {{ this }} TO analyst_limited",
      "GRANT SELECT ON {{ this }} TO analyst_full"
    ]
  )
}}

SELECT
  row_number() OVER () AS DeviceKey,
  Brand,
  Model,
  MinPower,
  InstallationDate,
  COALESCE(ValidTo, toDate('9999-12-31')) AS ValidTo
FROM {{ ref('stg_device') }}
WHERE Brand IS NOT NULL AND Model IS NOT NULL