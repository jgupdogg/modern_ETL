#!/usr/bin/env python3
"""
Analyze the improved silver PnL results to see if FIFO fixes worked
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

# Query latest silver PnL data
print('=== Latest Silver PnL Analysis (Post-Improved FIFO) ===')
result = conn.execute("""
    SELECT 
        time_period,
        COUNT(*) as total_records,
        SUM(CASE WHEN realized_pnl = 0 AND unrealized_pnl = 0 AND total_pnl = 0 THEN 1 ELSE 0 END) as zero_pnl_records,
        ROUND(100.0 * SUM(CASE WHEN realized_pnl = 0 AND unrealized_pnl = 0 AND total_pnl = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as zero_pnl_percentage,
        SUM(CASE WHEN total_pnl > 0 THEN 1 ELSE 0 END) as profitable_records,
        SUM(CASE WHEN total_pnl < 0 THEN 1 ELSE 0 END) as loss_records,
        ROUND(AVG(trade_count), 2) as avg_trade_count,
        ROUND(AVG(realized_pnl), 2) as avg_realized_pnl,
        ROUND(MAX(total_pnl), 2) as max_pnl,
        ROUND(MIN(total_pnl), 2) as min_pnl
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address != 'ALL_TOKENS'
    GROUP BY time_period
    ORDER BY time_period
""").fetchall()

for row in result:
    print(f'Timeframe: {row[0]}, Total: {row[1]}, Zero PnL: {row[2]} ({row[3]}%), Profitable: {row[4]}, Loss: {row[5]}, Avg Trades: {row[6]}, Avg PnL: {row[7]}, Max: {row[8]}, Min: {row[9]}')

print('\n=== Sample Non-Zero PnL Records ===')
sample_result = conn.execute("""
    SELECT wallet_address, token_address, time_period, realized_pnl, unrealized_pnl, total_pnl, trade_count, win_rate
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address != 'ALL_TOKENS' AND total_pnl != 0
    ORDER BY ABS(total_pnl) DESC
    LIMIT 10
""").fetchall()

for row in sample_result:
    print(f'Wallet: {row[0][:10]}..., Token: {row[1][:10]}..., Period: {row[2]}, Realized: ${row[3]:.2f}, Unrealized: ${row[4]:.2f}, Total: ${row[5]:.2f}, Trades: {row[6]}, Win Rate: {row[7]:.1f}%')

print('\n=== Processing Status Check ===')
status_result = conn.execute("""
    SELECT 
        time_period,
        batch_id,
        COUNT(*) as records_created,
        MAX(processed_at) as latest_processing
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address != 'ALL_TOKENS'
    GROUP BY time_period, batch_id
    ORDER BY latest_processing DESC
    LIMIT 5
""").fetchall()

for row in status_result:
    print(f'Period: {row[0]}, Batch: {row[1]}, Records: {row[2]}, Processed: {row[3]}')

print('\n=== SELL-before-BUY Success Analysis ===')
sell_buy_result = conn.execute("""
    SELECT 
        time_period,
        COUNT(*) as total_records,
        SUM(CASE WHEN trade_count > 0 AND realized_pnl != 0 THEN 1 ELSE 0 END) as records_with_trades_and_pnl,
        ROUND(100.0 * SUM(CASE WHEN trade_count > 0 AND realized_pnl != 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_percentage
    FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    WHERE token_address != 'ALL_TOKENS' AND trade_count > 0
    GROUP BY time_period
    ORDER BY time_period
""").fetchall()

for row in sell_buy_result:
    print(f'Period: {row[0]}, Total w/ Trades: {row[1]}, PnL Calculated: {row[2]}, Success Rate: {row[3]}%')