
        
            delete from "analytics"."main"."silver_transaction_details"
            where (
                message_id) in (
                select (message_id)
                from "silver_transaction_details__dbt_tmp20250611125631557103"
            );

        
    

    insert into "analytics"."main"."silver_transaction_details" ("message_id", "event_timestamp", "event_type", "transaction_type", "transaction_amount", "token_a", "token_b", "account_keys", "transaction_signature", "transaction_fee", "processing_date", "silver_processed_at")
    (
        select "message_id", "event_timestamp", "event_type", "transaction_type", "transaction_amount", "token_a", "token_b", "account_keys", "transaction_signature", "transaction_fee", "processing_date", "silver_processed_at"
        from "silver_transaction_details__dbt_tmp20250611125631557103"
    )
  