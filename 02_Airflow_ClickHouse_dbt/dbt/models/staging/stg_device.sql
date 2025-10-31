{{
  config(
    materialized='view'
  )
}}

-- Staging: device model info from bronze bronze_iot_raw_data.bronze_device
select
  row_number() over () as DeviceKey,
  *
from {{ source('bronze_layer', 'bronze_device') }}
