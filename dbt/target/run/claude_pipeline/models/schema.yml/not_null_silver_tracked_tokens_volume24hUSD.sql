
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select volume24hUSD
from "analytics"."main"."silver_tracked_tokens"
where volume24hUSD is null



  
  
      
    ) dbt_internal_test