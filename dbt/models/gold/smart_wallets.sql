{{
    config(
        materialized='table',
        unique_key=['wallet_address'],
        on_schema_change='fail',
        post_hook="COPY (SELECT * FROM {{ this }}) TO 's3://{{ var('solana_bucket') }}/gold/smart_wallets/smart_wallets.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);"
    )
}}

WITH silver_wallet_pnl AS (
    SELECT *
    FROM read_parquet('s3://{{ var("solana_bucket") }}/silver/wallet_pnl/**/*.parquet')
),

filtered_smart_wallets AS (
    SELECT 
        wallet_address,
        trade_count,
        trade_frequency_daily,
        avg_holding_time_hours,
        avg_transaction_amount_usd,
        total_bought,
        total_sold,
        win_rate,
        first_transaction,
        last_transaction,
        current_position_tokens,
        current_position_cost_basis,
        current_position_value,
        realized_pnl,
        unrealized_pnl,
        total_pnl,
        roi,
        batch_id,
        processed_at,
        data_source
    FROM silver_wallet_pnl
    WHERE 
        -- Focus on portfolio-level records only (simplified schema)
        token_address = 'ALL_TOKENS' 
        
        -- Updated gold criteria matching smart_trader_config.py
        AND total_pnl >= 10.0           -- MIN_TOTAL_PNL = 10.0
        AND roi >= 1.0                  -- MIN_ROI_PERCENT = 1.0
        AND win_rate >= 40.0            -- MIN_WIN_RATE_PERCENT = 40.0
        AND trade_count >= 1            -- MIN_TRADE_COUNT = 1
        AND total_pnl > 0               -- Must be profitable
        
),

ranked_smart_wallets AS (
    SELECT 
        wallet_address,
        trade_count,
        trade_frequency_daily,
        avg_holding_time_hours,
        avg_transaction_amount_usd,
        total_bought,
        total_sold,
        win_rate,
        first_transaction,
        last_transaction,
        current_position_tokens,
        current_position_cost_basis,
        current_position_value,
        realized_pnl,
        unrealized_pnl,
        total_pnl,
        roi,
        batch_id,
        processed_at,
        data_source,
        -- Calculate smart trader score based on profitability and trading patterns
        ROUND(
            CASE 
                WHEN total_pnl > 0 THEN 
                    (LEAST(total_pnl / 10000.0, 1.0) * 0.4) +           -- Profitability (40%)
                    (LEAST(win_rate / 100.0, 1.0) * 0.3) +              -- Win rate (30%)
                    (GREATEST(1.0 - (trade_frequency_daily / 50.0), 0.1) * 0.3)  -- Lower frequency bonus (30%)
                ELSE 0.1  -- Minimum score for break-even traders
            END,
            3
        ) as smart_trader_score,
        -- Rank wallets by profitability
        ROW_NUMBER() OVER (
            ORDER BY total_pnl DESC, win_rate DESC
        ) as profitability_rank
    FROM filtered_smart_wallets
)

SELECT 
    wallet_address,
    trade_count,
    trade_frequency_daily,
    avg_holding_time_hours,
    avg_transaction_amount_usd,
    total_bought,
    total_sold,
    win_rate,
    first_transaction,
    last_transaction,
    current_position_tokens,
    current_position_cost_basis,
    current_position_value,
    realized_pnl,
    unrealized_pnl,
    total_pnl,
    roi,
    smart_trader_score,
    profitability_rank,
    batch_id,
    processed_at,
    data_source,
    -- Processing metadata
    CURRENT_TIMESTAMP as gold_processed_at,
    'smart_wallet_filtered' as gold_processing_status,
    -- Performance tier classification matching smart_trader_config.py
    CASE 
        WHEN total_pnl >= 1000 AND roi >= 30 AND win_rate >= 60 AND trade_count >= 10 THEN 'elite'
        WHEN total_pnl >= 100 AND roi >= 15 AND win_rate >= 40 AND trade_count >= 5 THEN 'strong'
        ELSE 'promising'
    END as performance_tier
FROM ranked_smart_wallets
ORDER BY profitability_rank