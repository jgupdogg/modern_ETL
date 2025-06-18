#!/usr/bin/env python3
"""
Analyze UNKNOWN transactions to understand why they can't be classified
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

print('=== UNKNOWN Transaction Analysis ===')

# 1. Sample UNKNOWN transactions to see their structure
print('\n1. Sample UNKNOWN transactions:')
unknown_result = conn.execute("""
    SELECT 
        wallet_address,
        token_address,
        from_address,
        to_address,
        from_amount,
        to_amount,
        from_symbol,
        to_symbol,
        value_usd,
        timestamp
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    WHERE transaction_type = 'UNKNOWN'
    LIMIT 10
""").fetchall()

for i, row in enumerate(unknown_result):
    print(f'{i+1}. Wallet: {row[0][:10]}...')
    print(f'   Target Token: {row[1][:10]}...')
    print(f'   From: {row[6]} ({row[2][:10]}...) Amount: {row[4]}')
    print(f'   To: {row[7]} ({row[3][:10]}...) Amount: {row[5]}')
    print(f'   Value USD: {row[8]}, Time: {row[9]}')
    print()

# 2. Check why they're UNKNOWN - neither token matches target
print('2. UNKNOWN transaction token matching analysis:')
matching_result = conn.execute("""
    SELECT 
        COUNT(*) as total_unknown,
        SUM(CASE WHEN from_address = token_address THEN 1 ELSE 0 END) as from_matches_target,
        SUM(CASE WHEN to_address = token_address THEN 1 ELSE 0 END) as to_matches_target,
        SUM(CASE WHEN from_address != token_address AND to_address != token_address THEN 1 ELSE 0 END) as neither_matches
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    WHERE transaction_type = 'UNKNOWN'
""").fetchall()

row = matching_result[0]
print(f'Total UNKNOWN: {row[0]}')
print(f'From address matches target token: {row[1]} ({100*row[1]/row[0]:.1f}%)')
print(f'To address matches target token: {row[2]} ({100*row[2]/row[0]:.1f}%)')
print(f'Neither matches target token: {row[3]} ({100*row[3]/row[0]:.1f}%)')

# 3. Check if these are multi-hop swaps or different token pairs
print('\n3. Token pair analysis for UNKNOWN transactions:')
pair_result = conn.execute("""
    SELECT 
        from_symbol,
        to_symbol,
        COUNT(*) as count
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    WHERE transaction_type = 'UNKNOWN'
    GROUP BY from_symbol, to_symbol
    ORDER BY count DESC
    LIMIT 10
""").fetchall()

for row in pair_result:
    print(f'{row[0]} -> {row[1]}: {row[2]} transactions')

# 4. Check if UNKNOWN transactions involve stablecoins or intermediate tokens
print('\n4. Common tokens in UNKNOWN transactions:')
token_result = conn.execute("""
    WITH all_tokens AS (
        SELECT from_symbol as symbol, from_address as address FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet') WHERE transaction_type = 'UNKNOWN'
        UNION ALL
        SELECT to_symbol as symbol, to_address as address FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet') WHERE transaction_type = 'UNKNOWN'
    )
    SELECT 
        symbol,
        address,
        COUNT(*) as occurrences
    FROM all_tokens
    GROUP BY symbol, address
    ORDER BY occurrences DESC
    LIMIT 10
""").fetchall()

for row in token_result:
    print(f'{row[0]} ({row[1][:10]}...): {row[2]} occurrences')

# 5. Check specific examples where wallet might be swapping pre-existing holdings
print('\n5. Example of potential pre-existing holding swap:')
example_result = conn.execute("""
    SELECT 
        w1.wallet_address,
        w1.token_address as target_token,
        w1.transaction_type,
        w1.from_symbol,
        w1.to_symbol,
        w1.from_amount,
        w1.to_amount,
        w1.timestamp,
        w1.transaction_hash
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet') w1
    WHERE w1.transaction_type = 'UNKNOWN'
    AND EXISTS (
        SELECT 1 
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet') w2
        WHERE w2.wallet_address = w1.wallet_address
        AND w2.token_address = w1.token_address
        AND w2.transaction_type IN ('BUY', 'SELL')
    )
    LIMIT 5
""").fetchall()

print('Wallets with UNKNOWN transactions that also have BUY/SELL for same token:')
for row in example_result:
    print(f'Wallet: {row[0][:10]}..., Target: {row[1][:10]}...')
    print(f'  Type: {row[2]}, {row[3]} -> {row[4]}')
    print(f'  Amounts: {row[5]} -> {row[6]}')
    print(f'  Hash: {row[8][:10]}...')