

-- Silver layer: Transaction-specific details for Solana events
SELECT 
    message_id,
    event_timestamp,
    event_type,
    
    -- Extract transaction-specific fields from JSON payload
    CASE 
        WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.transactionType')
        ELSE NULL
    END as transaction_type,
    
    CASE 
        WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.transactionAmount')
        ELSE NULL
    END as transaction_amount,
    
    CASE 
        WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.tokenA')
        ELSE NULL
    END as token_a,
    
    CASE 
        WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.tokenB')
        ELSE NULL
    END as token_b,
    
    CASE 
        WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.accountKeys')
        ELSE NULL
    END as account_keys,
    
    CASE 
        WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.signature')
        ELSE NULL
    END as transaction_signature,
    
    CASE 
        WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.fee')
        ELSE NULL
    END as transaction_fee,
    
    -- Processing metadata
    processing_date,
    silver_processed_at
    
FROM "analytics"."main"."silver_webhook_events"
WHERE event_type IN ('solana_swap', 'solana_transaction')


  -- Only process new transactions since last run
  AND silver_processed_at > (SELECT MAX(silver_processed_at) FROM "analytics"."main"."silver_transaction_details")
