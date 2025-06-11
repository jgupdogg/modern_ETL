
    
    

select
    message_id as unique_field,
    count(*) as n_records

from "analytics"."main"."silver_transaction_details"
where message_id is not null
group by message_id
having count(*) > 1


