
    
    



select transaction_signature
from (select * from "analytics"."main"."silver_transaction_details" where transaction_signature IS NOT NULL) dbt_subquery
where transaction_signature is null


