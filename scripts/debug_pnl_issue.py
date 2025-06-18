#!/usr/bin/env python3
"""
Debug the PnL calculation issues by examining raw transaction data
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

print('=== Raw Transaction Data Analysis ===')

# Check the wallet with the extreme PnL value
print('\n1. Examining wallet with $3.67T unrealized PnL:')
extreme_wallet = '3prfs721McPj1Z8Rg1iLZ5XJZffJo5JtEyGr6eN4cHC'
extreme_token = '9SucTN9ngHKaJNq1NQQ1sQhq1eFGG9mzKFAPRVRsyf47'

txn_result = conn.execute(f"""
    SELECT 
        transaction_type,
        from_amount,
        to_amount,
        base_price,
        quote_price,
        value_usd,
        timestamp,
        block_unix_time
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    WHERE wallet_address = '{extreme_wallet}' 
    AND token_address = '{extreme_token}'
    ORDER BY timestamp
    LIMIT 10
""").fetchall()

print(f'Transactions for wallet {extreme_wallet[:10]}... token {extreme_token[:10]}...:')
for i, row in enumerate(txn_result):
    print(f'  {i+1}. Type: {row[0]}, From: {row[1]}, To: {row[2]}, Base Price: {row[3]}, Quote Price: {row[4]}, Value USD: {row[5]}, Time: {row[6]}')

if len(txn_result) == 0:
    print('  No transactions found - this explains zero trade count!')

print('\n2. Check for data quality issues in bronze layer:')
quality_result = conn.execute("""
    SELECT 
        COUNT(*) as total_transactions,
        COUNT(CASE WHEN value_usd IS NULL OR value_usd = 0 THEN 1 END) as missing_usd_value,
        COUNT(CASE WHEN base_price IS NULL OR base_price = 0 THEN 1 END) as missing_base_price,
        COUNT(CASE WHEN quote_price IS NULL OR quote_price = 0 THEN 1 END) as missing_quote_price,
        COUNT(CASE WHEN transaction_type = 'UNKNOWN' THEN 1 END) as unknown_type,
        AVG(CASE WHEN value_usd > 0 THEN value_usd END) as avg_value_usd,
        MAX(value_usd) as max_value_usd,
        MIN(value_usd) as min_value_usd
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
""").fetchall()

row = quality_result[0]
print(f'Total transactions: {row[0]}')
print(f'Missing USD value: {row[1]} ({100*row[1]/row[0]:.1f}%)')
print(f'Missing base price: {row[2]} ({100*row[2]/row[0]:.1f}%)')
print(f'Missing quote price: {row[3]} ({100*row[3]/row[0]:.1f}%)')
print(f'Unknown transaction type: {row[4]} ({100*row[4]/row[0]:.1f}%)')
print(f'Avg USD value: ${row[5]:.2f}')
print(f'Max USD value: ${row[6]:.2f}')
print(f'Min USD value: ${row[7]:.2f}')

print('\n3. Sample of actual transactions with valid prices:')
sample_result = conn.execute("""
    SELECT 
        wallet_address,
        token_address,
        transaction_type,
        from_amount,
        to_amount,
        value_usd,
        base_price,
        quote_price
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    WHERE value_usd > 0 AND value_usd < 1000000
    ORDER BY value_usd DESC
    LIMIT 10
""").fetchall()

for i, row in enumerate(sample_result):
    print(f'  {i+1}. Wallet: {row[0][:10]}..., Token: {row[1][:10]}..., Type: {row[2]}, From: {row[3]}, To: {row[4]}, Value: ${row[5]:.2f}, Base Price: {row[6]}, Quote Price: {row[7]}')

print('\n4. Check for missing transaction matching in PnL calculations:')
unmatched_result = conn.execute("""
    SELECT 
        wallet_address,
        token_address,
        COUNT(*) as bronze_transactions,
        MAX(processed_for_pnl) as is_processed_for_pnl
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    GROUP BY wallet_address, token_address
    HAVING COUNT(*) > 5
    ORDER BY COUNT(*) DESC
    LIMIT 5
""").fetchall()

print('Wallets with most transactions:')
for row in unmatched_result:
    print(f'  Wallet: {row[0][:10]}..., Token: {row[1][:10]}..., Transactions: {row[2]}, Processed for PnL: {row[3]}')