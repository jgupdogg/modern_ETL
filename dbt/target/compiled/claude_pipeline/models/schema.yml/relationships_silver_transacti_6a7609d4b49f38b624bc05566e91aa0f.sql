
    
    

with child as (
    select message_id as from_field
    from "analytics"."main"."silver_transaction_details"
    where message_id is not null
),

parent as (
    select message_id as to_field
    from "analytics"."main"."silver_webhook_events"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


