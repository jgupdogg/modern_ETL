{{
    config(
        materialized='table',
        unique_key=['wallet_address'],
        on_schema_change='fail',
        post_hook="COPY (SELECT * FROM {{ this }}) TO 's3://smart-trader/gold/smart_wallets/enhanced_smart_wallets_{{ run_started_at.strftime('%Y%m%d_%H%M%S') }}.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);"
    )
}}

-- Enhanced Smart Wallets Gold Layer
-- Uses enhanced silver layer data with quality metrics and trade analytics
-- Implements minimal but meaningful criteria for top wallet identification

WITH enhanced_silver_pnl AS (
    -- Load enhanced silver wallet PnL data from smart-trader bucket
    SELECT *
    FROM parquet_scan('s3://smart-trader/silver/wallet_pnl/**/*.parquet')
),

-- Step 1: Apply minimal but meaningful filtering criteria
filtered_wallets AS (
    SELECT 
        wallet_address,
        token_address,
        
        -- Core PnL metrics
        realized_pnl,
        unrealized_pnl,
        total_pnl,
        
        -- Trading performance metrics
        total_trades,
        win_rate,
        roi,
        
        -- Enhanced analytics (NEW - from our enhanced silver layer)
        avg_trade_efficiency,
        avg_timing_score,
        pnl_quality_score,
        
        -- Position data
        current_position_tokens,
        current_price_estimate,
        current_position_value_usd,
        total_cost_basis,
        
        -- Time analytics
        first_trade_date,
        last_trade_date,
        trading_period_days,
        trade_frequency,
        
        -- Processing metadata
        calculation_date,
        processed_at,
        batch_id
        
    FROM enhanced_silver_pnl
    WHERE 
        -- Minimal criteria adjusted for current data
        total_pnl >= 0                          -- Include break-even traders for now
        AND total_trades >= 1                   -- Any trading activity
        AND pnl_quality_score >= 0.5           -- Basic data quality
        AND avg_trade_efficiency >= 0.7        -- Good execution quality
        AND token_address IS NOT NULL          -- Valid token data
        AND wallet_address IS NOT NULL         -- Valid wallet data
),

-- Step 2: Calculate enhanced smart trader scores
scored_wallets AS (
    SELECT 
        *,
        
        -- Enhanced Smart Trader Score Algorithm
        ROUND(
            -- Profitability factor (40% weight)
            (LEAST(total_pnl / 1000.0, 1.0) * 0.4) +
            
            -- Trade quality factor (30% weight) - using enhanced analytics
            (COALESCE(avg_trade_efficiency, 0.5) * 0.3) +
            
            -- Timing factor (20% weight) - using enhanced analytics  
            (COALESCE(avg_timing_score, 0.5) * 0.2) +
            
            -- Consistency factor (10% weight)
            (CASE 
                WHEN total_trades >= 10 THEN 0.1
                WHEN total_trades >= 5 THEN 0.07
                WHEN total_trades >= 3 THEN 0.05
                ELSE 0.02
            END),
            3
        ) as enhanced_smart_trader_score,
        
        -- ROI-based performance tier (adjusted for current data)
        CASE 
            WHEN total_pnl > 0 AND roi > 0 AND win_rate > 0.5 AND total_trades >= 5 THEN 'ELITE'
            WHEN total_pnl > 0 AND roi > 0 AND win_rate > 0.3 AND total_trades >= 3 THEN 'STRONG'  
            WHEN total_pnl >= 0 AND avg_trade_efficiency >= 0.8 AND total_trades >= 1 THEN 'PROMISING'
            ELSE 'DEVELOPING'
        END as performance_tier,
        
        -- Trade execution quality tier (NEW - based on enhanced analytics)
        CASE 
            WHEN avg_trade_efficiency >= 0.9 AND avg_timing_score >= 0.8 THEN 'EXCELLENT_EXECUTION'
            WHEN avg_trade_efficiency >= 0.8 AND avg_timing_score >= 0.7 THEN 'GOOD_EXECUTION'
            WHEN avg_trade_efficiency >= 0.7 AND avg_timing_score >= 0.6 THEN 'AVERAGE_EXECUTION'
            ELSE 'IMPROVING_EXECUTION'
        END as execution_quality_tier
        
    FROM filtered_wallets
),

-- Step 3: Rank and add portfolio-level analytics
ranked_wallets AS (
    SELECT 
        *,
        
        -- Ranking metrics
        ROW_NUMBER() OVER (ORDER BY enhanced_smart_trader_score DESC, total_pnl DESC) as overall_rank,
        ROW_NUMBER() OVER (ORDER BY total_pnl DESC) as profitability_rank,
        ROW_NUMBER() OVER (ORDER BY avg_trade_efficiency DESC, avg_timing_score DESC) as execution_rank,
        ROW_NUMBER() OVER (ORDER BY roi DESC) as roi_rank,
        
        -- Percentile rankings
        PERCENT_RANK() OVER (ORDER BY enhanced_smart_trader_score) as score_percentile,
        PERCENT_RANK() OVER (ORDER BY total_pnl) as pnl_percentile,
        PERCENT_RANK() OVER (ORDER BY avg_trade_efficiency) as efficiency_percentile,
        
        -- Portfolio value analysis
        CASE 
            WHEN current_position_value_usd >= 100000 THEN 'LARGE_PORTFOLIO'
            WHEN current_position_value_usd >= 10000 THEN 'MEDIUM_PORTFOLIO'  
            WHEN current_position_value_usd >= 1000 THEN 'SMALL_PORTFOLIO'
            ELSE 'MICRO_PORTFOLIO'
        END as portfolio_size_tier
        
    FROM scored_wallets
),

-- Step 4: Add token-level aggregations for multi-token traders
wallet_summary AS (
    SELECT 
        wallet_address,
        
        -- Aggregate metrics across all tokens for this wallet
        COUNT(*) as tokens_traded,
        SUM(total_pnl) as total_portfolio_pnl,
        AVG(enhanced_smart_trader_score) as avg_smart_trader_score,
        MAX(enhanced_smart_trader_score) as best_token_score,
        
        -- Portfolio diversification metrics
        COUNT(CASE WHEN total_pnl > 0 THEN 1 END) as profitable_tokens,
        COUNT(CASE WHEN total_pnl < 0 THEN 1 END) as losing_tokens,
        
        -- Quality and execution aggregates
        AVG(pnl_quality_score) as avg_portfolio_quality,
        AVG(avg_trade_efficiency) as avg_portfolio_efficiency,
        AVG(avg_timing_score) as avg_portfolio_timing,
        
        -- Best performing token details
        (ARRAY_AGG(token_address ORDER BY total_pnl DESC))[1] as best_performing_token,
        MAX(total_pnl) as best_token_pnl,
        
        -- Processing metadata
        MAX(processed_at) as latest_processed_at,
        COUNT(DISTINCT batch_id) as data_batches
        
    FROM ranked_wallets
    GROUP BY wallet_address
)

-- Final SELECT: Combine individual token performance with portfolio summary
SELECT 
    rw.wallet_address,
    rw.token_address,
    
    -- Individual token performance
    rw.total_pnl,
    rw.realized_pnl,
    rw.unrealized_pnl,
    rw.roi,
    rw.win_rate,
    rw.total_trades,
    
    -- Enhanced analytics
    rw.avg_trade_efficiency,
    rw.avg_timing_score,
    rw.pnl_quality_score,
    rw.enhanced_smart_trader_score,
    
    -- Classifications
    rw.performance_tier,
    rw.execution_quality_tier,
    rw.portfolio_size_tier,
    
    -- Rankings
    rw.overall_rank,
    rw.profitability_rank,
    rw.execution_rank,
    rw.score_percentile,
    rw.pnl_percentile,
    rw.efficiency_percentile,
    
    -- Portfolio-level metrics (from wallet_summary)
    ws.tokens_traded,
    ws.total_portfolio_pnl,
    ws.avg_smart_trader_score,
    ws.profitable_tokens,
    ws.losing_tokens,
    ws.avg_portfolio_quality,
    ws.avg_portfolio_efficiency,
    ws.best_performing_token,
    ws.best_token_pnl,
    
    -- Position and timing data
    rw.current_position_value_usd,
    rw.trading_period_days,
    rw.trade_frequency,
    rw.first_trade_date,
    rw.last_trade_date,
    
    -- Processing metadata
    rw.calculation_date,
    rw.processed_at,
    CURRENT_TIMESTAMP as gold_processed_at,
    'enhanced_smart_wallets_v2' as gold_model_version,
    rw.batch_id as source_batch_id
    
FROM ranked_wallets rw
LEFT JOIN wallet_summary ws ON rw.wallet_address = ws.wallet_address

-- Order by enhanced smart trader score (best traders first)
ORDER BY rw.enhanced_smart_trader_score DESC, rw.total_pnl DESC

-- Optional: Limit to top performers for initial testing
-- LIMIT 100