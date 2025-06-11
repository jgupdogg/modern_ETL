
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    processing_date as unique_field,
    count(*) as n_records

from "analytics"."main"."silver_data_quality_metrics"
where processing_date is not null
group by processing_date
having count(*) > 1



  
  
      
    ) dbt_internal_test