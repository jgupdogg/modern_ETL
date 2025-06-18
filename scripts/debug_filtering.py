#!/usr/bin/env python3
"""
Debug the filtering logic to understand why transactions are being excluded
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

print('=== Quality Filter Analysis ===')

# Step 1: Total bronze transactions
total_result = conn.execute("""
    SELECT COUNT(*) as total_transactions
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
""").fetchall()
print(f'1. Total bronze transactions: {total_result[0][0]}')

# Step 2: Unprocessed transactions
unprocessed_result = conn.execute("""
    SELECT COUNT(*) as unprocessed_transactions
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    WHERE processed_for_pnl = false
""").fetchall()
print(f'2. Unprocessed transactions (processed_for_pnl = false): {unprocessed_result[0][0]}')

# Step 3: Apply each filter step by step
steps = [
    ("transaction_hash IS NOT NULL", "transaction_hash IS NOT NULL"),
    ("wallet_address IS NOT NULL", "wallet_address IS NOT NULL"),
    ("token_address IS NOT NULL", "token_address IS NOT NULL"),
    ("timestamp IS NOT NULL", "timestamp IS NOT NULL"),
    ("(from_amount IS NOT NULL AND from_amount > 0) OR (to_amount IS NOT NULL AND to_amount > 0)", 
     "(from_amount IS NOT NULL AND from_amount > 0) OR (to_amount IS NOT NULL AND to_amount > 0)")
]

base_filter = "processed_for_pnl = false"
cumulative_filter = base_filter

for i, (description, filter_condition) in enumerate(steps):
    cumulative_filter += f" AND {filter_condition}"
    
    step_result = conn.execute(f"""
        SELECT COUNT(*) as remaining_transactions
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
        WHERE {cumulative_filter}
    """).fetchall()
    
    print(f'3.{i+1}. After adding "{description}": {step_result[0][0]} transactions remain')

# Step 4: Check what's failing each filter
print('\n=== Detailed Filter Failures ===')

failures = [
    ("Missing transaction_hash", "transaction_hash IS NULL"),
    ("Missing wallet_address", "wallet_address IS NULL"),
    ("Missing token_address", "token_address IS NULL"),
    ("Missing timestamp", "timestamp IS NULL"),
    ("Missing amounts", "(from_amount IS NULL OR from_amount <= 0) AND (to_amount IS NULL OR to_amount <= 0)")
]

for description, condition in failures:
    failure_result = conn.execute(f"""
        SELECT COUNT(*) as failed_count
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
        WHERE processed_for_pnl = false AND {condition}
    """).fetchall()
    
    print(f'{description}: {failure_result[0][0]} transactions')

# Step 5: Examine specific failing records
print('\n=== Sample of Records Failing Amount Filter ===')
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
    WHERE processed_for_pnl = false 
    AND (from_amount IS NULL OR from_amount <= 0) 
    AND (to_amount IS NULL OR to_amount <= 0)
    LIMIT 5
""").fetchall()

for i, row in enumerate(sample_result):
    print(f'  {i+1}. Wallet: {row[0][:10]}..., Type: {row[2]}, From: {row[3]}, To: {row[4]}, Value: {row[5]}, Base Price: {row[6]}, Quote Price: {row[7]}')

# Step 6: Check transaction type distribution
print('\n=== Transaction Type Distribution ===')
type_result = conn.execute("""
    SELECT 
        transaction_type,
        COUNT(*) as count,
        COUNT(CASE WHEN from_amount > 0 OR to_amount > 0 THEN 1 END) as valid_amounts
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    WHERE processed_for_pnl = false
    GROUP BY transaction_type
    ORDER BY count DESC
""").fetchall()

for row in type_result:
    print(f'{row[0]}: {row[1]} total, {row[2]} with valid amounts ({100*row[2]/row[1]:.1f}%)')