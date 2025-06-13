
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select whale_fetch_status
from "analytics"."main"."silver_tracked_tokens"
where whale_fetch_status is null



  
  
      
    ) dbt_internal_test