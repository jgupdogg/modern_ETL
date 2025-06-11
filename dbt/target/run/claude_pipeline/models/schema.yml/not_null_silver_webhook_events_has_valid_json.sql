
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_valid_json
from "analytics"."main"."silver_webhook_events"
where has_valid_json is null



  
  
      
    ) dbt_internal_test