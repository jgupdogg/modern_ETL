
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quality_score
from "analytics"."main"."silver_tracked_tokens"
where quality_score is null



  
  
      
    ) dbt_internal_test