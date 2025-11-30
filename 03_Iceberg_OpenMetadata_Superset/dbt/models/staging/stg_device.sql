{{
  config(
    materialized='view'
  )
}}

select
  row_number() over () as DeviceKey,
  *
from {{ source('bronze_layer', 'bronze_device') }}
