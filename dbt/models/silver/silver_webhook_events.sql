{{ config(
    materialized='incremental',
    unique_key='message_id',
    on_schema_change='fail'
) }}

-- Silver layer: Cleaned and categorized webhook events
SELECT 
    message_id,
    
    -- Standardize timestamp format
    CASE 
        WHEN webhook_timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'
        THEN CAST(webhook_timestamp AS TIMESTAMP)
        ELSE NULL
    END as event_timestamp,
    
    source_ip,
    file_path,
    
    -- Parse JSON payload safely
    CASE 
        WHEN JSON_VALID(payload) THEN payload::JSON
        ELSE NULL
    END as payload_json,
    
    -- Extract specific fields from payload
    CASE 
        WHEN JSON_VALID(payload) THEN JSON_EXTRACT(payload, '$.source')
        ELSE NULL
    END as payload_source,
    
    CASE 
        WHEN JSON_VALID(payload) THEN JSON_EXTRACT(payload, '$.test')
        ELSE NULL
    END as payload_test,
    
    CASE 
        WHEN JSON_VALID(payload) THEN JSON_EXTRACT(payload, '$.value')
        ELSE NULL
    END as payload_value,
    
    -- Categorize event types
    CASE 
        WHEN payload LIKE '%transaction%' AND payload LIKE '%SWAP%' THEN 'solana_swap'
        WHEN payload LIKE '%transaction%' THEN 'solana_transaction'
        WHEN payload LIKE '%test%' THEN 'test_event'
        ELSE 'unknown'
    END as event_type,
    
    -- Quality flags
    CASE WHEN JSON_VALID(payload) THEN true ELSE false END as has_valid_json,
    CASE WHEN source_ip IS NOT NULL THEN true ELSE false END as has_source_ip,
    
    -- Processing metadata
    kafka_timestamp,
    processed_at,
    processing_date,
    
    -- Add silver processing timestamp
    CURRENT_TIMESTAMP as silver_processed_at
    
FROM {{ ref('bronze_webhooks') }}
WHERE row_num = 1  -- Only include deduplicated records

{% if is_incremental() %}
  -- Only process new data since last run
  AND processed_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}