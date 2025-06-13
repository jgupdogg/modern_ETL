
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price24hChangePercent
from "analytics"."main"."silver_tracked_tokens"
where price24hChangePercent is null



  
  
      
    ) dbt_internal_test