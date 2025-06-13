
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        processing_status as value_field,
        count(*) as n_records

    from "analytics"."main"."bronze_tokens"
    group by processing_status

)

select *
from all_values
where value_field not in (
    'unprocessed','silver_processed'
)



  
  
      
    ) dbt_internal_test