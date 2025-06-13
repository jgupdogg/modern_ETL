
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        whale_fetch_status as value_field,
        count(*) as n_records

    from "analytics"."main"."silver_tracked_tokens"
    group by whale_fetch_status

)

select *
from all_values
where value_field not in (
    'pending','fetched','failed'
)



  
  
      
    ) dbt_internal_test