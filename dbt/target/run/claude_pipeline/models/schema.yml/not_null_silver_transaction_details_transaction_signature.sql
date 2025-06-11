
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_signature
from (select * from "analytics"."main"."silver_transaction_details" where transaction_signature IS NOT NULL) dbt_subquery
where transaction_signature is null



  
  
      
    ) dbt_internal_test