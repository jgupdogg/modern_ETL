
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select logoURI
from "analytics"."main"."silver_tracked_tokens"
where logoURI is null



  
  
      
    ) dbt_internal_test