
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "analytics"."main"."silver_tracked_tokens"

where not(volume24hUSD >= 50000)


  
  
      
    ) dbt_internal_test