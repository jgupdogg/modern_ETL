{{
    config(
        materialized='table',
        unique_key='wallet_address',
        file_format='delta',
        location_root='s3a://smart-trader/gold/smart_traders_delta'
    )
}}

WITH delta_silver_wallet_pnl AS (
    -- Read from TRUE Delta Lake silver wallet PnL table using Spark
    SELECT *
    FROM delta.`s3a://{{ var("smart_trader_bucket") }}/silver/wallet_pnl`
),

qualified_traders AS (
    SELECT 
        wallet_address,
        total_pnl,
        portfolio_roi,
        win_rate,
        trade_count,
        total_bought,
        total_sold,
        current_position_cost_basis,
        current_position_value,
        realized_pnl,
        unrealized_pnl,
        avg_holding_time_hours,
        trade_frequency_daily,
        first_transaction,
        last_transaction,
        calculation_date,
        batch_id,
        processed_at,
        data_source
    FROM delta_silver_wallet_pnl
    WHERE 
        -- Smart trader qualification criteria: profitable or successful
        (win_rate > 0 OR total_pnl > 0)
        
        -- Additional quality filters
        AND trade_count >= 1  -- Must have at least 1 trade
        AND total_bought > 0  -- Must have meaningful trading volume
        AND wallet_address IS NOT NULL
        AND calculation_date IS NOT NULL
),

enhanced_smart_traders AS (
    SELECT 
        -- Core trader identification
        wallet_address,
        
        -- Performance metrics
        total_pnl,
        portfolio_roi,
        win_rate,
        trade_count,
        total_bought,
        total_sold,
        realized_pnl,
        unrealized_pnl,
        current_position_cost_basis,
        current_position_value,
        
        -- Calculate performance tier based on combined metrics
        CASE 
            WHEN total_pnl >= 10000 AND portfolio_roi >= 50 AND win_rate >= 30 THEN 'ELITE'
            WHEN total_pnl >= 1000 AND portfolio_roi >= 20 AND win_rate >= 20 THEN 'STRONG' 
            WHEN total_pnl >= 100 AND portfolio_roi >= 10 AND win_rate >= 10 THEN 'PROMISING'
            WHEN total_pnl > 0 OR win_rate > 0 THEN 'QUALIFIED'
            ELSE 'UNQUALIFIED'
        END as performance_tier,
        
        -- Calculate smart trader score (weighted combination)
        ROUND(
            (CASE WHEN total_pnl > 0 THEN LEAST(total_pnl / 1000, 100) ELSE 0 END) * 0.4 +  -- 40% profitability (capped at 100)
            (CASE WHEN portfolio_roi > 0 THEN LEAST(portfolio_roi, 100) ELSE 0 END) * 0.3 +  -- 30% ROI (capped at 100%)
            (CASE WHEN win_rate > 0 THEN win_rate ELSE 0 END) * 0.2 +                        -- 20% win rate
            (CASE WHEN trade_count > 10 THEN 10 ELSE trade_count END) * 0.1,                 -- 10% activity bonus (capped at 10)
            2
        ) as smart_trader_score,
        
        -- Trading behavior metrics
        avg_holding_time_hours,
        trade_frequency_daily,
        first_transaction,
        last_transaction,
        
        -- Calculate trading experience in days
        DATEDIFF(last_transaction, first_transaction) + 1 as trading_experience_days,
        
        -- Calculate average trade size
        ROUND((total_bought + total_sold) / trade_count, 2) as avg_trade_size_usd,
        
        -- Risk metrics
        CASE 
            WHEN total_bought > 0 THEN ROUND((total_sold / total_bought) * 100, 2)
            ELSE 0 
        END as sell_ratio_percent,
        
        -- Metadata
        calculation_date,
        batch_id as source_batch_id,
        processed_at as source_processed_at,
        data_source as source_data_source,
        current_timestamp() as gold_created_at,
        
        -- Add row number for ranking within performance tiers
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN total_pnl >= 10000 AND portfolio_roi >= 50 AND win_rate >= 30 THEN 'ELITE'
                    WHEN total_pnl >= 1000 AND portfolio_roi >= 20 AND win_rate >= 20 THEN 'STRONG' 
                    WHEN total_pnl >= 100 AND portfolio_roi >= 10 AND win_rate >= 10 THEN 'PROMISING'
                    WHEN total_pnl > 0 OR win_rate > 0 THEN 'QUALIFIED'
                    ELSE 'UNQUALIFIED'
                END
            ORDER BY smart_trader_score DESC, total_pnl DESC
        ) as tier_rank
    FROM qualified_traders
),

final_smart_traders AS (
    SELECT 
        wallet_address,
        
        -- Performance metrics
        total_pnl,
        portfolio_roi,
        win_rate,
        trade_count,
        total_bought,
        total_sold,
        realized_pnl,
        unrealized_pnl,
        current_position_cost_basis,
        current_position_value,
        
        -- Smart trader classification
        performance_tier,
        smart_trader_score,
        tier_rank,
        
        -- Trading behavior
        avg_holding_time_hours,
        trade_frequency_daily,
        trading_experience_days,
        avg_trade_size_usd,
        sell_ratio_percent,
        
        -- Timestamps
        first_transaction,
        last_transaction,
        calculation_date,
        gold_created_at,
        
        -- Metadata
        source_batch_id,
        source_processed_at,
        source_data_source,
        
        -- Calculate overall rank across all tiers
        ROW_NUMBER() OVER (ORDER BY smart_trader_score DESC, total_pnl DESC) as overall_rank
    FROM enhanced_smart_traders
)

SELECT 
    wallet_address,
    performance_tier,
    smart_trader_score,
    overall_rank,
    tier_rank,
    total_pnl,
    portfolio_roi,
    win_rate,
    trade_count,
    total_bought,
    total_sold,
    realized_pnl,
    unrealized_pnl,
    current_position_cost_basis,
    current_position_value,
    avg_holding_time_hours,
    trade_frequency_daily,
    trading_experience_days,
    avg_trade_size_usd,
    sell_ratio_percent,
    first_transaction,
    last_transaction,
    calculation_date,
    gold_created_at,
    source_batch_id,
    source_processed_at,
    source_data_source
FROM final_smart_traders
ORDER BY smart_trader_score DESC, total_pnl DESC