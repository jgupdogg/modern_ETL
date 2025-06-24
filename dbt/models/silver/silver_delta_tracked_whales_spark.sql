{{
    config(
        materialized='table',
        unique_key='whale_id',
        file_format='delta',
        location_root='s3a://smart-trader/silver/tracked_whales_delta'
    )
}}

WITH delta_bronze_whales AS (
    -- Read from TRUE Delta Lake bronze whale holders table using Spark
    SELECT *
    FROM delta.`s3a://{{ var("smart_trader_bucket") }}/bronze/whale_holders`
),

basic_filtered_whales AS (
    SELECT 
        token_address,
        token_symbol,
        token_name,
        wallet_address,
        rank,
        holdings_amount,
        holdings_value_usd,
        holdings_percentage,
        txns_fetched,
        txns_last_fetched_at,
        txns_fetch_status,
        fetched_at,
        batch_id,
        data_source,
        _delta_timestamp,
        _delta_operation,
        rank_date,
        _delta_created_at
    FROM delta_bronze_whales
    WHERE 
        -- Basic validation filters
        wallet_address IS NOT NULL
        AND token_address IS NOT NULL
        AND holdings_amount > 0  -- Only whales with actual token holdings
        
        -- Only process the latest Delta Lake operations
        AND _delta_operation IN ('BRONZE_WHALE_CREATE', 'BRONZE_WHALE_APPEND')
),

enhanced_silver_whales AS (
    SELECT 
        -- Create composite whale ID for unique tracking
        CONCAT(wallet_address, '_', token_address) as whale_id,
        
        -- Core whale information
        wallet_address,
        token_address,
        token_symbol,
        token_name,
        rank,
        holdings_amount,
        holdings_value_usd,
        holdings_percentage,
        
        -- Calculate whale tier based on holdings amount (token quantity)
        CASE 
            WHEN holdings_amount >= 1000000 THEN 'MEGA'
            WHEN holdings_amount >= 100000 THEN 'LARGE' 
            WHEN holdings_amount >= 10000 THEN 'MEDIUM'
            WHEN holdings_amount >= 1000 THEN 'SMALL'
            ELSE 'MINIMAL'
        END as whale_tier,
        
        -- Calculate rank tier for easier filtering
        CASE 
            WHEN rank <= 3 THEN 'TOP_3'
            WHEN rank <= 10 THEN 'TOP_10'
            WHEN rank <= 50 THEN 'TOP_50'
            ELSE 'OTHER'
        END as rank_tier,
        
        -- Transaction tracking status
        txns_fetched,
        txns_last_fetched_at,
        COALESCE(txns_fetch_status, 'pending') as txns_fetch_status,
        
        -- Silver layer metadata
        rank_date,
        batch_id as bronze_batch_id,
        _delta_timestamp as bronze_delta_timestamp,
        _delta_operation as source_operation,
        _delta_created_at as bronze_created_at,
        current_timestamp() as silver_created_at,
        
        -- Tracking fields for downstream processing
        CASE 
            WHEN txns_fetched = true THEN 'completed'
            WHEN txns_fetch_status = 'pending' THEN 'pending'
            ELSE 'ready'
        END as processing_status,
        
        true as is_newly_tracked,
        
        -- Row number for deduplication (keep latest by rank_date and bronze timestamp)
        ROW_NUMBER() OVER (
            PARTITION BY wallet_address, token_address 
            ORDER BY rank_date DESC, _delta_timestamp DESC
        ) as rn
    FROM basic_filtered_whales
)

SELECT 
    whale_id,
    wallet_address,
    token_address,
    token_symbol,
    token_name,
    rank,
    holdings_amount,
    holdings_value_usd,
    holdings_percentage,
    whale_tier,
    rank_tier,
    txns_fetched,
    txns_last_fetched_at,
    txns_fetch_status,
    processing_status,
    rank_date,
    bronze_batch_id,
    bronze_delta_timestamp,
    source_operation,
    bronze_created_at,
    silver_created_at,
    is_newly_tracked
FROM enhanced_silver_whales
WHERE rn = 1  -- Keep only latest version of each whale-token pair
ORDER BY holdings_amount DESC, rank ASC