{{
    config(
        materialized='table',
        unique_key='token_address',
        file_format='delta',
        location_root='s3a://smart-trader/silver/tracked_tokens_delta'
    )
}}

WITH delta_bronze_tokens AS (
    -- Read from TRUE Delta Lake bronze token metrics table using Spark
    SELECT *
    FROM delta.`s3a://{{ var("smart_trader_bucket") }}/bronze/token_metrics`
),

basic_filtered_tokens AS (
    SELECT 
        token_address,
        symbol,
        name,
        decimals,
        liquidity,
        price,
        fdv,
        holder,
        extensions,
        _delta_timestamp,
        _delta_operation,
        processing_date,
        batch_id,
        _delta_created_at,
        whale_fetch_status,
        whale_fetched_at,
        is_newly_tracked
    FROM delta_bronze_tokens
    WHERE 
        -- Basic liquidity filter - only tokens with meaningful liquidity
        liquidity >= 100000  -- Min $100K liquidity for silver layer
        
        -- Ensure we have core required fields
        AND token_address IS NOT NULL
        AND symbol IS NOT NULL
        AND liquidity IS NOT NULL
        AND price IS NOT NULL
        
        -- INCREMENTAL PROCESSING: Only process unprocessed tokens
        AND (whale_fetch_status = 'pending' OR whale_fetch_status IS NULL)
        
        -- Only process the latest Delta Lake operations
        AND _delta_operation IN ('BRONZE_TOKEN_CREATE', 'BRONZE_TOKEN_APPEND', 'WHALE_STATUS_UPDATE')
),

enhanced_silver_tokens AS (
    SELECT 
        token_address,
        symbol,
        name,
        decimals,
        liquidity,
        price,
        fdv,
        holder,
        
        -- Calculate liquidity tier for categorization
        CASE 
            WHEN liquidity >= 1000000 THEN 'HIGH'
            WHEN liquidity >= 500000 THEN 'MEDIUM' 
            WHEN liquidity >= 100000 THEN 'LOW'
            ELSE 'MINIMAL'
        END as liquidity_tier,
        
        -- Calculate holder density (FDV per holder)
        CASE 
            WHEN holder > 0 THEN ROUND(fdv / holder, 2)
            ELSE NULL 
        END as fdv_per_holder,
        
        -- Silver layer metadata
        processing_date,
        batch_id as bronze_batch_id,
        _delta_timestamp as bronze_delta_timestamp,
        _delta_operation as source_operation,
        _delta_created_at as bronze_created_at,
        current_timestamp() as silver_created_at,
        
        -- Tracking fields for downstream processing (preserve from bronze)
        whale_fetch_status,
        whale_fetched_at,
        is_newly_tracked,
        
        -- Row number for deduplication (keep latest by processing_date)
        ROW_NUMBER() OVER (
            PARTITION BY token_address 
            ORDER BY processing_date DESC, _delta_timestamp DESC
        ) as rn
    FROM basic_filtered_tokens
)

SELECT 
    token_address,
    symbol,
    name,
    decimals,
    liquidity,
    price,
    fdv,
    holder,
    liquidity_tier,
    fdv_per_holder,
    processing_date,
    bronze_batch_id,
    bronze_delta_timestamp,
    source_operation,
    bronze_created_at,
    silver_created_at,
    whale_fetch_status,
    whale_fetched_at,
    is_newly_tracked
FROM enhanced_silver_tokens
WHERE rn = 1  -- Keep only latest version of each token
ORDER BY liquidity DESC