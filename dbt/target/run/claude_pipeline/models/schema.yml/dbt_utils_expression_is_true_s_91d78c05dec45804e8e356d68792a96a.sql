
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "analytics"."main"."silver_data_quality_metrics"

where not(total_events >= 0)


  
  
      
    ) dbt_internal_test