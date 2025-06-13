
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rank
from "analytics"."main"."silver_tracked_tokens"
where rank is null



  
  
      
    ) dbt_internal_test