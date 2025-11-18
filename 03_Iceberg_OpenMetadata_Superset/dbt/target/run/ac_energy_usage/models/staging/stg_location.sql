

  create view `default`.`stg_location` 
  
    
    
  as (
    

-- Staging: device location SCD from bronze raw_data.location_data
select
  row_number() over () as LocationKey,
  *
from `default`.`bronze_location`
  )
      
      
                    -- end_of_sql
                    
                    