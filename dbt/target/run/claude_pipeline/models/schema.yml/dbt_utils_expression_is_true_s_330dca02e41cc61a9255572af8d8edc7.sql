
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from "analytics"."main"."silver_tracked_tokens" where volume_mcap_ratio IS NOT NULL) dbt_subquery

where not(volume_mcap_ratio >= 0.05)


  
  
      
    ) dbt_internal_test