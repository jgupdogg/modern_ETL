#!/usr/bin/env python3
"""
Test the updated gold layer criteria
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

print('=== Updated Gold Layer Criteria Test ===')

# Updated criteria from config
MIN_TOTAL_PNL = 10.0         # $10 minimum PnL (more realistic)
MIN_ROI_PERCENT = 1.0        # 1% minimum ROI
MIN_WIN_RATE_PERCENT = 40.0  # 40% minimum win rate  
MIN_TRADE_COUNT = 1          # 1 minimum trade (adjusted)

print(f'Updated criteria: PnL >= ${MIN_TOTAL_PNL}, ROI >= {MIN_ROI_PERCENT}%, Win Rate >= {MIN_WIN_RATE_PERCENT}%, Trades >= {MIN_TRADE_COUNT}')

# Test updated criteria
criteria_result = conn.execute(f"""
    SELECT 
        COUNT(*) as total_candidates,
        COUNT(CASE WHEN 
            total_pnl >= {MIN_TOTAL_PNL} AND 
            roi >= {MIN_ROI_PERCENT} AND 
            win_rate >= {MIN_WIN_RATE_PERCENT} AND 
            trade_count >= {MIN_TRADE_COUNT} AND
            total_pnl > 0
        THEN 1 END) as qualifying_candidates,
        COUNT(CASE WHEN 
            total_pnl >= 100 AND 
            roi >= 15 AND 
            win_rate >= 40 AND 
            trade_count >= 5 AND
            total_pnl > 0
        THEN 1 END) as strong_tier_candidates,
        COUNT(CASE WHEN 
            total_pnl >= 1000 AND 
            roi >= 30 AND 
            win_rate >= 60 AND 
            trade_count >= 10 AND
            total_pnl > 0
        THEN 1 END) as elite_tier_candidates
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address = 'ALL_TOKENS' 
    AND time_period = 'all'
    AND processed_for_gold = false
""").fetchall()

row = criteria_result[0]
print(f'   Total unprocessed candidates: {row[0]}')
print(f'   Qualifying candidates (base criteria): {row[1]}')
print(f'   Strong tier candidates: {row[2]}')
print(f'   Elite tier candidates: {row[3]}')

if row[1] > 0:
    print(f'\n✅ Success! Found {row[1]} qualifying candidates for gold layer')
    
    # Show top candidates
    print('\nTop 10 qualifying candidates:')
    top_result = conn.execute(f"""
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
    
    for i, row in enumerate(top_result):
        # Determine tier
        if row[1] >= 1000 and row[2] >= 30 and row[3] >= 60 and row[4] >= 10:
            tier = "ELITE"
        elif row[1] >= 100 and row[2] >= 15 and row[3] >= 40 and row[4] >= 5:
            tier = "STRONG"
        else:
            tier = "PROMISING"
        
        print(f'   {i+1}. {row[0][:10]}... | PnL: ${row[1]:.2f} | ROI: {row[2]:.2f}% | Win Rate: {row[3]:.2f}% | Trades: {row[4]} | Tier: {tier}')

else:
    print('❌ Still no qualifying candidates with updated criteria')