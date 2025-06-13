
    
    

select
    token_address as unique_field,
    count(*) as n_records

from "analytics"."main"."silver_tracked_tokens"
where token_address is not null
group by token_address
having count(*) > 1


