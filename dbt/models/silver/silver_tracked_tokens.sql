{{
    config(
        materialized='incremental',
        unique_key='token_address',
        on_schema_change='fail',
        post_hook="COPY (SELECT * FROM {{ this }}) TO 's3://{{ var('solana_bucket') }}/{{ var('token_silver_path') }}/tracked_tokens.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);"
    )
}}

WITH filtered_tokens AS (
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
        price_change_24h_percent,
        batch_id,
        ingested_at,
        processing_date
    FROM {{ ref('bronze_tokens') }}
    WHERE 
        -- Quality filters
        logo_uri IS NOT NULL 
        AND logo_uri != ''
        AND liquidity >= 10000  -- Min $10K liquidity
        AND volume_24h_usd >= 50000  -- Min $50K volume
        AND price_change_24h_percent >= 30  -- Min 30% price change
        -- All price changes must be positive (strong momentum)
        AND price_change_1h_percent > 0
        AND price_change_2h_percent > 0
        AND price_change_4h_percent > 0
        AND price_change_8h_percent > 0
        AND price_change_24h_percent > 0
        {% if is_incremental() %}
        -- Only process new/updated data
        AND processing_date > (SELECT MAX(processing_date) FROM {{ this }})
        {% endif %}
),

calculated_metrics AS (
    SELECT 
        token_address,
        symbol,
        name,
        decimals,
        logo_uri as logoURI,
        liquidity,
        price,
        fdv,
        market_cap as marketcap,
        volume_24h_usd as volume24hUSD,
        volume_24h_change_percent as volume24hChangePercent,
        price_change_24h_percent as price24hChangePercent,
        -- Calculate volume/market cap ratio
        CASE 
            WHEN market_cap > 0 THEN volume_24h_usd / market_cap 
            ELSE NULL 
        END as volume_mcap_ratio,
        batch_id as bronze_id,
        ingested_at,
        processing_date
    FROM filtered_tokens
    WHERE 
        -- Filter by volume/mcap ratio if market cap exists
        (market_cap IS NULL OR market_cap <= 0 OR 
         (volume_24h_usd / market_cap) >= 0.05)  -- Min 5% volume/mcap ratio
),

ranked_tokens AS (
    SELECT 
        token_address,
        symbol,
        name,
        decimals,
        logoURI,
        liquidity,
        price,
        fdv,
        marketcap,
        volume24hUSD,
        volume24hChangePercent,
        price24hChangePercent,
        volume_mcap_ratio,
        bronze_id,
        ingested_at,
        processing_date,
        -- Rank by liquidity (top tokens get priority)
        ROW_NUMBER() OVER (
            PARTITION BY processing_date 
            ORDER BY liquidity DESC
        ) as liquidity_rank
    FROM calculated_metrics
)

SELECT 
    token_address,
    symbol,
    name,
    decimals,
    logoURI,
    volume24hUSD,
    volume24hChangePercent,
    price24hChangePercent,
    marketcap,
    liquidity,
    price,
    fdv,
    liquidity_rank as rank,
    volume_mcap_ratio,
    -- Calculate quality score based on multiple factors
    ROUND(
        (LEAST(price24hChangePercent / 100.0, 1.0) * 0.4) +  -- Price momentum (40%)
        (LEAST(volume_mcap_ratio * 10, 1.0) * 0.3) +         -- Volume activity (30%)
        (LEAST(liquidity / 100000.0, 1.0) * 0.3),            -- Liquidity depth (30%)
        3
    ) as quality_score,
    bronze_id,
    ingested_at as created_at,
    CURRENT_TIMESTAMP as updated_at,
    processing_date
FROM ranked_tokens
WHERE liquidity_rank <= 50  -- Top 50 tokens by liquidity