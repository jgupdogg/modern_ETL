
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "analytics"."main"."silver_data_quality_metrics"

where not(json_validity_pct json_validity_pct >= 0 AND json_validity_pct <= 100)


  
  
      
    ) dbt_internal_test