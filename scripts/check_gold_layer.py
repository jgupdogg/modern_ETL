#!/usr/bin/env python3
"""
Check if the gold layer is properly processing silver PnL data
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

print('=== Gold Layer Analysis ===')

# 1. Check if gold layer data exists
try:
    gold_result = conn.execute("""
        SELECT COUNT(*) as total_records
        FROM read_parquet('s3://solana-data/gold/top_traders/**/*.parquet')
    """).fetchall()
    print(f'1. Gold layer records found: {gold_result[0][0]}')
    
    if gold_result[0][0] > 0:
        # 2. Check gold layer structure and recent data
        structure_result = conn.execute("""
            SELECT 
                COUNT(*) as total_traders,
                COUNT(DISTINCT wallet_address) as unique_wallets,
                MAX(calculation_date) as latest_date,
                MAX(processed_at) as latest_processing,
                AVG(total_pnl) as avg_total_pnl,
                MAX(total_pnl) as max_pnl,
                MIN(total_pnl) as min_pnl
            FROM read_parquet('s3://solana-data/gold/top_traders/**/*.parquet')
        """).fetchall()
        
        row = structure_result[0]
        print(f'2. Gold layer summary:')
        print(f'   Total trader records: {row[0]}')
        print(f'   Unique wallets: {row[1]}')
        print(f'   Latest calculation: {row[2]}')
        print(f'   Latest processing: {row[3]}')
        print(f'   Avg PnL: ${row[4]:.2f}')
        print(f'   Max PnL: ${row[5]:.2f}')
        print(f'   Min PnL: ${row[6]:.2f}')
        
        # 3. Show top traders
        print('\n3. Top 10 traders by PnL:')
        top_result = conn.execute("""
            SELECT 
                wallet_address,
                total_pnl,
                total_bought,
                total_sold,
                trade_count,
                win_rate,
                performance_tier
            FROM read_parquet('s3://solana-data/gold/top_traders/**/*.parquet')
            ORDER BY total_pnl DESC
            LIMIT 10
        """).fetchall()
        
        for i, row in enumerate(top_result):
            print(f'   {i+1}. {row[0][:10]}... | PnL: ${row[1]:.2f} | Bought: ${row[2]:.2f} | Sold: ${row[3]:.2f} | Trades: {row[4]} | Win Rate: {row[5]:.1f}% | Tier: {row[6]}')
        
        # 4. Check performance tier distribution
        print('\n4. Performance tier distribution:')
        tier_result = conn.execute("""
            SELECT 
                performance_tier,
                COUNT(*) as count,
                AVG(total_pnl) as avg_pnl,
                AVG(win_rate) as avg_win_rate
            FROM read_parquet('s3://solana-data/gold/top_traders/**/*.parquet')
            GROUP BY performance_tier
            ORDER BY avg_pnl DESC
        """).fetchall()
        
        for row in tier_result:
            print(f'   {row[0]}: {row[1]} traders, Avg PnL: ${row[2]:.2f}, Avg Win Rate: {row[3]:.1f}%')

except Exception as e:
    print(f'1. No gold layer data found or error: {e}')
    print('   This suggests the gold_top_traders task may have failed or not run yet')

# 5. Check silver->gold processing status
print('\n5. Silver PnL processing status for gold:')
silver_status = conn.execute("""
    SELECT 
        COUNT(*) as total_silver_records,
        SUM(CASE WHEN processed_for_gold = true THEN 1 ELSE 0 END) as processed_for_gold,
        SUM(CASE WHEN processed_for_gold = false THEN 1 ELSE 0 END) as unprocessed_for_gold,
        MAX(processed_at) as latest_silver_processing
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address = 'ALL_TOKENS'  -- Portfolio level records
""").fetchall()

row = silver_status[0]
print(f'   Total silver portfolio records: {row[0]}')
print(f'   Processed for gold: {row[1]}')
print(f'   Unprocessed for gold: {row[2]}')
print(f'   Latest silver processing: {row[3]}')

# 6. Check if there are qualifying traders for gold layer
print('\n6. Potential gold candidates from silver:')
candidates_result = conn.execute("""
    SELECT 
        COUNT(*) as potential_candidates,
        COUNT(CASE WHEN total_pnl > 0 THEN 1 END) as profitable_candidates,
        COUNT(CASE WHEN total_pnl > 1000 THEN 1 END) as high_profit_candidates,
        MAX(total_pnl) as max_candidate_pnl
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address = 'ALL_TOKENS' 
    AND time_period = 'all'
    AND trade_count > 0
""").fetchall()

row = candidates_result[0]
print(f'   Total candidates with trades: {row[0]}')
print(f'   Profitable candidates: {row[1]}')
print(f'   High profit candidates (>$1000): {row[2]}')
print(f'   Max candidate PnL: ${row[3]:.2f}')