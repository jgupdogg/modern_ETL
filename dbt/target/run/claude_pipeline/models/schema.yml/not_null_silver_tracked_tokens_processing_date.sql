
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select processing_date
from "analytics"."main"."silver_tracked_tokens"
where processing_date is null



  
  
      
    ) dbt_internal_test