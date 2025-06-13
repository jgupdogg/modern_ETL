
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select token_address
from "analytics"."main"."bronze_tokens"
where token_address is null



  
  
      
    ) dbt_internal_test