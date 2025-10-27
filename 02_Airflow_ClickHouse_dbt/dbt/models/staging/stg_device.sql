{{
  config(
    materialized='view'
  )
}}

-- Staging: device model info from bronze raw_data.device_data
select
  *
from {{ source('raw_data', 'device_data') }}
