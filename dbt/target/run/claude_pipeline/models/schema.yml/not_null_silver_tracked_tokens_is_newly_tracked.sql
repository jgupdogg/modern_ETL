
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_newly_tracked
from "analytics"."main"."silver_tracked_tokens"
where is_newly_tracked is null



  
  
      
    ) dbt_internal_test