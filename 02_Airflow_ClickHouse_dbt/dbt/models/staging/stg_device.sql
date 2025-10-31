{{
  config(
    materialized='view'
  )
}}

-- Staging: device model info from bronze raw_data.device_data
select
  row_number() over () as DeviceKey,
  *
from {{ source('bronze_iot_raw_data', 'bronze_device') }}
