
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from "analytics"."main"."silver_webhook_events"
    group by event_type

)

select *
from all_values
where value_field not in (
    'solana_swap','solana_transaction','test_event','unknown'
)


