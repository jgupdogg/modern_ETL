
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "analytics"."main"."silver_data_quality_metrics"

where not(unique_message_ids <= total_events)


  
  
      
    ) dbt_internal_test