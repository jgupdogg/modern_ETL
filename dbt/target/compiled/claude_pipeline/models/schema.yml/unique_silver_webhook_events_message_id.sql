
    
    

select
    message_id as unique_field,
    count(*) as n_records

from "analytics"."main"."silver_webhook_events"
where message_id is not null
group by message_id
having count(*) > 1


