
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select processing_date
from "analytics"."main"."silver_webhook_events"
where processing_date is null



  
  
      
    ) dbt_internal_test