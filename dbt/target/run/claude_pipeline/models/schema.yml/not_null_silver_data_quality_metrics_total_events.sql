
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_events
from "analytics"."main"."silver_data_quality_metrics"
where total_events is null



  
  
      
    ) dbt_internal_test