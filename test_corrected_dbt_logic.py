#!/usr/bin/env python3
"""
Test the corrected DBT gold criteria logic (matching actual silver schema)
"""
import duckdb

# Test the corrected DBT logic
print('🎯 TESTING CORRECTED DBT GOLD CRITERIA LOGIC')
print('='*70)

# Connect to DuckDB
conn = duckdb.connect('/data/analytics.duckdb')
conn.execute('INSTALL httpfs;')
conn.execute('LOAD httpfs;')
conn.execute('SET s3_endpoint="minio:9000";')
conn.execute('SET s3_access_key_id="minioadmin";')
conn.execute('SET s3_secret_access_key="minioadmin123";')
conn.execute('SET s3_use_ssl=false;')
conn.execute('SET s3_url_style="path";')

print('🔍 Testing corrected dbt model logic...')

try:
    # Apply the CORRECTED filtering logic (matching actual schema)
    corrected_gold_test = conn.execute("""
    WITH silver_wallet_pnl AS (
        SELECT *
        FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    ),
    
    filtered_smart_wallets AS (
        SELECT 
            wallet_address,
            time_period,
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
            -- Focus on portfolio-level records for 'all' timeframe
            token_address = 'ALL_TOKENS' 
            AND time_period = 'all'
            
            -- Updated gold criteria matching smart_trader_config.py
            AND total_pnl >= 10.0           -- MIN_TOTAL_PNL = 10.0
            AND roi >= 1.0                  -- MIN_ROI_PERCENT = 1.0
            AND win_rate >= 40.0            -- MIN_WIN_RATE_PERCENT = 40.0
            AND trade_count >= 1            -- MIN_TRADE_COUNT = 1
            AND total_pnl > 0               -- Must be profitable
    ),
    
    tier_classification AS (
        SELECT 
            *,
            -- Performance tier classification matching smart_trader_config.py
            CASE 
                WHEN total_pnl >= 1000 AND roi >= 30 AND win_rate >= 60 AND trade_count >= 10 THEN 'elite'
                WHEN total_pnl >= 100 AND roi >= 15 AND win_rate >= 40 AND trade_count >= 5 THEN 'strong'
                ELSE 'promising'
            END as performance_tier,
            -- Smart trader score
            ROUND(
                CASE 
                    WHEN total_pnl > 0 THEN 
                        (LEAST(total_pnl / 10000.0, 1.0) * 0.4) +           -- Profitability (40%)
                        (LEAST(win_rate / 100.0, 1.0) * 0.3) +              -- Win rate (30%)
                        (GREATEST(1.0 - (trade_frequency_daily / 50.0), 0.1) * 0.3)  -- Lower frequency bonus (30%)
                    ELSE 0.1
                END,
                3
            ) as smart_trader_score,
            -- Rank by profitability
            ROW_NUMBER() OVER (ORDER BY total_pnl DESC, win_rate DESC) as profitability_rank
        FROM filtered_smart_wallets
    )
    
    SELECT 
        COUNT(*) as total_qualifying,
        COUNT(CASE WHEN performance_tier = 'elite' THEN 1 END) as elite_count,
        COUNT(CASE WHEN performance_tier = 'strong' THEN 1 END) as strong_count,
        COUNT(CASE WHEN performance_tier = 'promising' THEN 1 END) as promising_count,
        AVG(total_pnl) as avg_pnl,
        MAX(total_pnl) as max_pnl,
        AVG(roi) as avg_roi,
        AVG(win_rate) as avg_win_rate,
        AVG(trade_count) as avg_trades,
        AVG(smart_trader_score) as avg_score
    FROM tier_classification
    """).fetchone()
    
    if corrected_gold_test:
        total, elite, strong, promising, avg_pnl, max_pnl, avg_roi, avg_win_rate, avg_trades, avg_score = corrected_gold_test
        
        print(f'\n✅ CORRECTED DBT MODEL RESULTS:')
        print(f'   📊 Total Qualifying: {total}')
        print(f'   🏆 Elite Tier: {elite}')
        print(f'   💪 Strong Tier: {strong}')  
        print(f'   🌟 Promising Tier: {promising}')
        if avg_pnl is not None:
            print(f'   💰 Average PnL: ${avg_pnl:.2f}')
            print(f'   🚀 Maximum PnL: ${max_pnl:.2f}')
            print(f'   📈 Average ROI: {avg_roi:.2f}%')
            print(f'   🎯 Average Win Rate: {avg_win_rate:.1f}%')
            print(f'   🔄 Average Trades: {avg_trades:.1f}')
            print(f'   ⭐ Average Score: {avg_score:.3f}')
        
        if total > 0:
            print(f'\n🎉 SUCCESS: Found {total} qualifying smart traders!')
            
            # Get the top candidates with full details
            top_candidates = conn.execute("""
            WITH silver_wallet_pnl AS (
                SELECT *
                FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
            ),
            
            filtered_smart_wallets AS (
                SELECT 
                    wallet_address,
                    trade_count,
                    win_rate,
                    total_pnl,
                    roi,
                    realized_pnl,
                    unrealized_pnl,
                    total_bought,
                    total_sold
                FROM silver_wallet_pnl
                WHERE 
                    token_address = 'ALL_TOKENS' 
                    AND time_period = 'all'
                    AND total_pnl >= 10.0
                    AND roi >= 1.0
                    AND win_rate >= 40.0
                    AND trade_count >= 1
                    AND total_pnl > 0
            )
            
            SELECT 
                wallet_address,
                total_pnl,
                realized_pnl,
                unrealized_pnl,
                roi,
                win_rate,
                trade_count,
                total_bought,
                total_sold,
                CASE 
                    WHEN total_pnl >= 1000 AND roi >= 30 AND win_rate >= 60 AND trade_count >= 10 THEN 'elite'
                    WHEN total_pnl >= 100 AND roi >= 15 AND win_rate >= 40 AND trade_count >= 5 THEN 'strong'
                    ELSE 'promising'
                END as performance_tier
            FROM filtered_smart_wallets
            ORDER BY total_pnl DESC
            """).fetchall()
            
            print(f'\n🏆 ALL QUALIFYING SMART TRADERS:')
            for i, (wallet, pnl, realized, unrealized, roi, win_rate, trades, bought, sold, tier) in enumerate(top_candidates):
                print(f'   {i+1}. {wallet[:12]}... | Tier: {tier.upper():<9} | Total PnL: ${pnl:.2f}')
                print(f'       Realized: ${realized:.2f} | Unrealized: ${unrealized:.2f} | ROI: {roi:.2f}%')
                print(f'       Win Rate: {win_rate:.1f}% | Trades: {trades} | Bought: ${bought:.2f} | Sold: ${sold:.2f}')
                print()
        
        else:
            print(f'\n❌ NO QUALIFYING CANDIDATES')

except Exception as e:
    print(f'❌ Error testing corrected DBT logic: {e}')

print(f'\n✅ Corrected DBT Logic Test Complete')
print(f'🎯 Ready for dbt transformation: silver → gold via corrected smart_wallets model')
conn.close()