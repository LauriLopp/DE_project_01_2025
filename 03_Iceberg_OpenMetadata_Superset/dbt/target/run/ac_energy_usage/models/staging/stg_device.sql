

  create view `default`.`stg_device__dbt_tmp` 
  
    
    
  as (
    

-- Staging: device model info from bronze bronze_iot_raw_data.bronze_device
select
  row_number() over () as DeviceKey,
  *
from `default`.`bronze_device`
  )
      
      
                    -- end_of_sql
                    
                    