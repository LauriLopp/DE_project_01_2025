{{
  config(
    materialized='view'
  )
}}

-- Staging: device location SCD from bronze raw_data.location_data
select
  row_number() over () as LocationKey,
  *
from {{ source('bronze_layer', 'bronze_location') }}
