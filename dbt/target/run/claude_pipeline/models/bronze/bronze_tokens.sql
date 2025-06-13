
  
  create view "analytics"."main"."bronze_tokens__dbt_tmp" as (
    

WITH raw_tokens AS (
    SELECT *
    FROM read_parquet('s3://solana-data/bronze/token_list_v3/**/*.parquet', union_by_name=true)
),

deduplicated_tokens AS (
    SELECT 
        token_address,
        symbol,
        name,
        decimals,
        logo_uri,
        liquidity,
        price,
        fdv,
        market_cap,
        volume_24h_usd,
        volume_24h_change_percent,
        price_change_1h_percent,
        price_change_2h_percent,
        price_change_4h_percent,
        price_change_8h_percent,
        price_change_24h_percent,
        ingested_at,
        batch_id,
        -- Add row number to handle duplicates - keep latest version
        ROW_NUMBER() OVER (
            PARTITION BY token_address 
            ORDER BY ingested_at DESC
        ) as rn
    FROM raw_tokens
    WHERE token_address IS NOT NULL
)

SELECT 
    token_address,
    symbol,
    name,
    decimals,
    logo_uri,
    liquidity,
    price,
    fdv,
    market_cap,
    volume_24h_usd,
    volume_24h_change_percent,
    price_change_1h_percent,
    price_change_2h_percent,
    price_change_4h_percent,
    price_change_8h_percent,
    price_change_24h_percent,
    ingested_at,
    batch_id,
    COALESCE(CAST(ingested_at AS DATE), CURRENT_DATE) as processing_date,
    -- Processing state tracking
    'unprocessed' as processing_status,
    NULL as silver_processed_at
FROM deduplicated_tokens
WHERE rn = 1  -- Keep only latest version of each token
  AND token_address IS NOT NULL
  );
