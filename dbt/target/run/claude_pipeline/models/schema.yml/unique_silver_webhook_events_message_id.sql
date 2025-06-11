
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    message_id as unique_field,
    count(*) as n_records

from "analytics"."main"."silver_webhook_events"
where message_id is not null
group by message_id
having count(*) > 1



  
  
      
    ) dbt_internal_test