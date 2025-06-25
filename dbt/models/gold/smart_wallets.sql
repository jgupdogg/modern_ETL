{{
    config(
        materialized='table',
        unique_key=['wallet_address'],
        on_schema_change='fail'
    )
}}

-- Simple gold layer: select wallets with positive PnL or positive win rate
WITH silver_wallet_pnl AS (
    SELECT *
    FROM delta_scan('s3a://smart-trader/silver/wallet_pnl')
),

smart_wallets AS (
    SELECT 
        wallet_address,
        trade_count,
        trade_frequency_daily,
        avg_holding_time_hours,
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
        portfolio_roi as roi,
        batch_id,
        processed_at,
        data_source,
        -- Simple performance classification
        CASE 
            WHEN total_pnl > 1000 AND win_rate > 50 THEN 'ELITE'
            WHEN total_pnl > 100 AND win_rate > 30 THEN 'STRONG'
            WHEN total_pnl > 0 OR win_rate > 0 THEN 'QUALIFIED'
            ELSE 'UNQUALIFIED'
        END as performance_tier,
        -- Processing metadata
        CURRENT_TIMESTAMP as gold_processed_at
    FROM silver_wallet_pnl
    WHERE 
        -- Focus on portfolio-level records only
        token_address = 'ALL_TOKENS' 
        
        -- Simple criteria: positive PnL OR positive win rate
        AND (total_pnl > 0 OR win_rate > 0)
)

SELECT *
FROM smart_wallets
ORDER BY total_pnl DESC, win_rate DESC