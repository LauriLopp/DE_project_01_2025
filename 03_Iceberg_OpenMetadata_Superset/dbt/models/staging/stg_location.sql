{{
  config(
    materialized='view'
  )
}}

select
  row_number() over () as LocationKey,
  *
from {{ source('bronze_layer', 'bronze_location') }}
