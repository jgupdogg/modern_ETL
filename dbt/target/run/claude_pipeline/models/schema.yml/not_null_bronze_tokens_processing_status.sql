
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select processing_status
from "analytics"."main"."bronze_tokens"
where processing_status is null



  
  
      
    ) dbt_internal_test