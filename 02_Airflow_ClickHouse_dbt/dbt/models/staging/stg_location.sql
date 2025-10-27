{{
  config(
    materialized='view'
  )
}}

-- Staging: device location SCD from bronze raw_data.location_data
select
  *
from {{ source('raw_data', 'location_data') }}
