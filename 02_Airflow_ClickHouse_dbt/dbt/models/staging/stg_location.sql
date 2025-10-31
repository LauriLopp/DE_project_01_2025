{{
  config(
    materialized='view'
  )
}}

-- Staging: device location SCD from bronze raw_data.location_data
select
  row_number() over () as LocationKey,
  *
from {{ source('raw_data', 'location_data') }}
