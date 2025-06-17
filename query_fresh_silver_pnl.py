#!/usr/bin/env python3
"""
Query the fresh silver PnL data and test gold criteria
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

print('=== Fresh Silver PnL Analysis ===')

# 1. Check what data was created
print('\n1. Basic data overview:')
basic_result = conn.execute("""
    SELECT 
        time_period,
        COUNT(*) as total_records,
        COUNT(DISTINCT wallet_address) as unique_wallets,
        SUM(CASE WHEN token_address = 'ALL_TOKENS' THEN 1 ELSE 0 END) as portfolio_records,
        SUM(CASE WHEN token_address != 'ALL_TOKENS' THEN 1 ELSE 0 END) as token_records,
        MAX(processed_at) as latest_processing
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    GROUP BY time_period
    ORDER BY time_period
""").fetchall()

for row in basic_result:
    print(f'   {row[0]}: {row[1]} total records, {row[2]} unique wallets, {row[3]} portfolio records, {row[4]} token records')

# 2. Focus on portfolio-level records (token_address = 'ALL_TOKENS') for 'all' timeframe
print('\n2. Portfolio-level PnL summary (for gold layer candidates):')
portfolio_result = conn.execute("""
    SELECT 
        COUNT(*) as total_portfolios,
        SUM(CASE WHEN total_pnl > 0 THEN 1 ELSE 0 END) as profitable_portfolios,
        SUM(CASE WHEN total_pnl < 0 THEN 1 ELSE 0 END) as loss_portfolios,
        SUM(CASE WHEN total_pnl = 0 THEN 1 ELSE 0 END) as zero_pnl_portfolios,
        SUM(CASE WHEN trade_count > 0 THEN 1 ELSE 0 END) as portfolios_with_trades,
        AVG(trade_count) as avg_trade_count,
        AVG(total_pnl) as avg_pnl,
        MAX(total_pnl) as max_pnl,
        MIN(total_pnl) as min_pnl
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address = 'ALL_TOKENS' 
    AND time_period = 'all'
""").fetchall()

row = portfolio_result[0]
print(f'   Total portfolios: {row[0]}')
print(f'   Profitable: {row[1]} ({100*row[1]/row[0]:.1f}%)')
print(f'   Loss: {row[2]} ({100*row[2]/row[0]:.1f}%)')
print(f'   Zero PnL: {row[3]} ({100*row[3]/row[0]:.1f}%)')
print(f'   With trades: {row[4]} ({100*row[4]/row[0]:.1f}%)')
print(f'   Avg trades: {row[5]:.2f}')
print(f'   Avg PnL: ${row[6]:.2f}')
print(f'   Max PnL: ${row[7]:.2f}')
print(f'   Min PnL: ${row[8]:.2f}')

# 3. Test our gold criteria
print('\n3. Testing gold criteria:')
MIN_TOTAL_PNL = 10.0
MIN_ROI_PERCENT = 1.0
MIN_WIN_RATE_PERCENT = 40.0
MIN_TRADE_COUNT = 1

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
""").fetchall()

candidates_row = criteria_result[0]
print(f'   Criteria: PnL >= ${MIN_TOTAL_PNL}, ROI >= {MIN_ROI_PERCENT}%, Win Rate >= {MIN_WIN_RATE_PERCENT}%, Trades >= {MIN_TRADE_COUNT}')
print(f'   Total candidates: {candidates_row[0]}')
print(f'   Qualifying candidates: {candidates_row[1]}')

# 4. Show qualifying candidates if any
if candidates_row[1] > 0:
    print(f'\n4. Top {min(10, candidates_row[1])} qualifying candidates:')
    top_result = conn.execute(f"""
        SELECT 
            wallet_address,
            total_pnl,
            realized_pnl,
            unrealized_pnl,
            roi,
            win_rate,
            trade_count,
            total_bought,
            total_sold
        FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
        WHERE token_address = 'ALL_TOKENS' 
        AND time_period = 'all'
        AND total_pnl >= {MIN_TOTAL_PNL} 
        AND roi >= {MIN_ROI_PERCENT} 
        AND win_rate >= {MIN_WIN_RATE_PERCENT} 
        AND trade_count >= {MIN_TRADE_COUNT}
        AND total_pnl > 0
        ORDER BY total_pnl DESC
        LIMIT 10
    """).fetchall()
    
    for i, row in enumerate(top_result):
        print(f'   {i+1}. {row[0][:10]}... | Total PnL: ${row[1]:.2f} | Realized: ${row[2]:.2f} | Unrealized: ${row[3]:.2f} | ROI: {row[4]:.2f}% | Win Rate: {row[5]:.1f}% | Trades: {row[6]} | Bought: ${row[7]:.2f} | Sold: ${row[8]:.2f}')

else:
    print('\n4. No qualifying candidates found. Checking why:')
    debug_result = conn.execute(f"""
        SELECT 
            COUNT(CASE WHEN total_pnl < {MIN_TOTAL_PNL} THEN 1 END) as fail_pnl,
            COUNT(CASE WHEN roi < {MIN_ROI_PERCENT} THEN 1 END) as fail_roi,
            COUNT(CASE WHEN win_rate < {MIN_WIN_RATE_PERCENT} THEN 1 END) as fail_win_rate,
            COUNT(CASE WHEN trade_count < {MIN_TRADE_COUNT} THEN 1 END) as fail_trade_count,
            COUNT(CASE WHEN total_pnl <= 0 THEN 1 END) as fail_profitability
        FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
        WHERE token_address = 'ALL_TOKENS' 
        AND time_period = 'all'
    """).fetchall()
    
    debug_row = debug_result[0]
    print(f'   Fail PnL threshold (< ${MIN_TOTAL_PNL}): {debug_row[0]}')
    print(f'   Fail ROI threshold (< {MIN_ROI_PERCENT}%): {debug_row[1]}')
    print(f'   Fail win rate threshold (< {MIN_WIN_RATE_PERCENT}%): {debug_row[2]}')
    print(f'   Fail trade count threshold (< {MIN_TRADE_COUNT}): {debug_row[3]}')
    print(f'   Fail profitability (<= $0): {debug_row[4]}')
    
    # Show best candidates even if they don't qualify
    print('\n   Best candidates (even if not qualifying):')
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
        AND trade_count > 0
        ORDER BY total_pnl DESC
        LIMIT 10
    """).fetchall()
    
    for i, row in enumerate(best_result):
        print(f'     {i+1}. {row[0][:10]}... | PnL: ${row[1]:.2f} | ROI: {row[2]:.2f}% | Win Rate: {row[3]:.2f}% | Trades: {row[4]} | Bought: ${row[5]:.2f} | Sold: ${row[6]:.2f}')

print(f'\n5. Data quality assessment:')
quality_result = conn.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN ABS(total_pnl) > 1000000 THEN 1 END) as extreme_pnl_records,
        COUNT(CASE WHEN roi > 1000 OR roi < -100 THEN 1 END) as extreme_roi_records,
        COUNT(CASE WHEN total_bought = 0 AND total_sold = 0 AND total_pnl != 0 THEN 1 END) as suspicious_records
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address = 'ALL_TOKENS' 
    AND time_period = 'all'
""").fetchall()

quality_row = quality_result[0]
print(f'   Total records: {quality_row[0]}')
print(f'   Extreme PnL (>$1M or <-$1M): {quality_row[1]} ({100*quality_row[1]/quality_row[0]:.1f}%)')
print(f'   Extreme ROI (>1000% or <-100%): {quality_row[2]} ({100*quality_row[2]/quality_row[0]:.1f}%)')
print(f'   Suspicious (no trades but PnL): {quality_row[3]} ({100*quality_row[3]/quality_row[0]:.1f}%)')

print('\n✅ Fresh silver PnL analysis complete!')