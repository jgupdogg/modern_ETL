#!/usr/bin/env python3
"""
Test the gold layer logic with current silver data
"""
import duckdb

# Connect to DuckDB
conn = duckdb.connect('/data/analytics.duckdb')

# Install httpfs extension for S3 access
conn.execute('INSTALL httpfs')
conn.execute('LOAD httpfs')

# Configure S3 settings for MinIO
conn.execute("SET s3_region='us-east-1'")
conn.execute("SET s3_use_ssl=false")
conn.execute("SET s3_url_style='path'")
conn.execute("SET s3_endpoint='minio:9000'")
conn.execute("SET s3_access_key_id='minioadmin'")
conn.execute("SET s3_secret_access_key='minioadmin123'")

print('=== Gold Layer Logic Test ===')

# 1. Check criteria matching based on default configuration
print('\n1. Testing default gold criteria from config:')

# Simulate gold criteria (from smart_trader_config.py defaults)
MIN_TOTAL_PNL = 100.0  # $100 minimum PnL
MIN_ROI_PERCENT = 5.0  # 5% minimum ROI
MIN_WIN_RATE_PERCENT = 40.0  # 40% minimum win rate  
MIN_TRADE_COUNT = 3  # 3 minimum trades

print(f'Criteria: PnL >= ${MIN_TOTAL_PNL}, ROI >= {MIN_ROI_PERCENT}%, Win Rate >= {MIN_WIN_RATE_PERCENT}%, Trades >= {MIN_TRADE_COUNT}')

# Test if any silver records meet these criteria
criteria_result = conn.execute(f"""
    SELECT 
        COUNT(*) as total_candidates,
        COUNT(CASE WHEN 
            total_pnl >= {MIN_TOTAL_PNL} AND 
            roi >= {MIN_ROI_PERCENT} AND 
            win_rate >= {MIN_WIN_RATE_PERCENT} AND 
            trade_count >= {MIN_TRADE_COUNT} AND
            total_pnl > 0
        THEN 1 END) as qualifying_candidates
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address = 'ALL_TOKENS' 
    AND time_period = 'all'
    AND processed_for_gold = false
""").fetchall()

row = criteria_result[0]
print(f'   Total unprocessed candidates: {row[0]}')
print(f'   Qualifying candidates: {row[1]}')

if row[1] == 0:
    print('\n2. Debugging why no candidates qualify:')
    
    # Check individual criteria failures
    debug_result = conn.execute(f"""
        SELECT 
            COUNT(CASE WHEN total_pnl < {MIN_TOTAL_PNL} THEN 1 END) as fail_pnl,
            COUNT(CASE WHEN roi < {MIN_ROI_PERCENT} THEN 1 END) as fail_roi,
            COUNT(CASE WHEN win_rate < {MIN_WIN_RATE_PERCENT} THEN 1 END) as fail_win_rate,
            COUNT(CASE WHEN trade_count < {MIN_TRADE_COUNT} THEN 1 END) as fail_trade_count,
            COUNT(CASE WHEN total_pnl <= 0 THEN 1 END) as fail_profitability,
            MAX(total_pnl) as max_pnl,
            MAX(roi) as max_roi,
            MAX(win_rate) as max_win_rate,
            MAX(trade_count) as max_trades
        FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
        WHERE token_address = 'ALL_TOKENS' 
        AND time_period = 'all'
        AND processed_for_gold = false
    """).fetchall()
    
    debug_row = debug_result[0]
    print(f'   Fail PnL threshold: {debug_row[0]}')
    print(f'   Fail ROI threshold: {debug_row[1]}')
    print(f'   Fail win rate threshold: {debug_row[2]}')
    print(f'   Fail trade count threshold: {debug_row[3]}')
    print(f'   Fail profitability (<=0): {debug_row[4]}')
    print(f'   Max PnL in data: ${debug_row[5]:.2f}')
    print(f'   Max ROI in data: {debug_row[6]:.2f}%')
    print(f'   Max win rate in data: {debug_row[7]:.2f}%')
    print(f'   Max trades in data: {debug_row[8]}')
    
    # Show best candidates even if they don't qualify
    print('\n3. Best candidates (even if not qualifying):')
    best_result = conn.execute("""
        SELECT 
            wallet_address,
            total_pnl,
            roi,
            win_rate,
            trade_count,
            total_bought,
            total_sold
        FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
        WHERE token_address = 'ALL_TOKENS' 
        AND time_period = 'all'
        AND processed_for_gold = false
        AND trade_count > 0
        ORDER BY total_pnl DESC
        LIMIT 10
    """).fetchall()
    
    for i, row in enumerate(best_result):
        print(f'   {i+1}. {row[0][:10]}... | PnL: ${row[1]:.2f} | ROI: {row[2]:.2f}% | Win Rate: {row[3]:.2f}% | Trades: {row[4]} | Bought: ${row[5]:.2f} | Sold: ${row[6]:.2f}')

else:
    print(f'\n2. Found {row[1]} qualifying candidates! Showing top 10:')
    qualifying_result = conn.execute(f"""
        SELECT 
            wallet_address,
            total_pnl,
            roi,
            win_rate,
            trade_count,
            total_bought,
            total_sold
        FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
        WHERE token_address = 'ALL_TOKENS' 
        AND time_period = 'all'
        AND processed_for_gold = false
        AND total_pnl >= {MIN_TOTAL_PNL} 
        AND roi >= {MIN_ROI_PERCENT} 
        AND win_rate >= {MIN_WIN_RATE_PERCENT} 
        AND trade_count >= {MIN_TRADE_COUNT}
        AND total_pnl > 0
        ORDER BY total_pnl DESC
        LIMIT 10
    """).fetchall()
    
    for i, row in enumerate(qualifying_result):
        print(f'   {i+1}. {row[0][:10]}... | PnL: ${row[1]:.2f} | ROI: {row[2]:.2f}% | Win Rate: {row[3]:.2f}% | Trades: {row[4]} | Bought: ${row[5]:.2f} | Sold: ${row[6]:.2f}')

# 4. Check configuration values that might be too strict
print('\n4. Suggested adjusted criteria based on actual data:')
stats_result = conn.execute("""
    SELECT 
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_pnl) as pnl_90th,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY roi) as roi_90th,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY win_rate) as win_rate_90th,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY trade_count) as trades_90th
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address = 'ALL_TOKENS' 
    AND time_period = 'all'
    AND processed_for_gold = false
    AND trade_count > 0
    AND total_pnl > 0
""").fetchall()

if stats_result:
    stats_row = stats_result[0]
    print(f'   Suggested PnL threshold (90th percentile): ${stats_row[0]:.2f}')
    print(f'   Suggested ROI threshold (90th percentile): {stats_row[1]:.2f}%')
    print(f'   Suggested win rate threshold (90th percentile): {stats_row[2]:.2f}%')
    print(f'   Suggested trade count threshold (90th percentile): {stats_row[3]:.0f}')