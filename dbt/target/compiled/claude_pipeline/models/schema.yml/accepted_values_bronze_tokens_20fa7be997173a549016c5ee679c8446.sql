
    
    

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


