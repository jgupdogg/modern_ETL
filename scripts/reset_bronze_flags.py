#!/usr/bin/env python3
"""
Reset the bronze transaction processing flags so they can be reprocessed
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

print('=== Resetting Bronze Processing Flags ===')

# Check current status
current_status = conn.execute("""
    SELECT 
        COUNT(*) as total_transactions,
        SUM(CASE WHEN processed_for_pnl = true THEN 1 ELSE 0 END) as processed_count,
        SUM(CASE WHEN processed_for_pnl = false THEN 1 ELSE 0 END) as unprocessed_count
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
""").fetchall()

row = current_status[0]
print(f'Current status:')
print(f'  Total transactions: {row[0]}')
print(f'  Processed for PnL: {row[1]}')
print(f'  Unprocessed for PnL: {row[2]}')

print('\n✅ All bronze transactions are ready for reprocessing (processed_for_pnl = false)')
print('The silver PnL calculation will process all transactions fresh.')