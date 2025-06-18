#!/usr/bin/env python3
"""
Analyze transaction hash patterns to understand the data structure
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

print('=== Transaction Hash Pattern Analysis ===')

# First check if we have real vs mock data by looking at hash patterns
print('\n1. Transaction hash pattern analysis:')
pattern_result = conn.execute("""
    SELECT 
        CASE 
            WHEN transaction_hash LIKE 'mock_tx_%' THEN 'Mock Data'
            WHEN LENGTH(transaction_hash) = 88 THEN 'Solana Transaction Hash'
            WHEN LENGTH(transaction_hash) = 64 THEN 'Other Blockchain Hash'
            ELSE 'Unknown Format'
        END as hash_type,
        COUNT(*) as count,
        COUNT(DISTINCT transaction_hash) as unique_hashes,
        MIN(transaction_hash) as example_hash
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    GROUP BY 1
    ORDER BY count DESC
""").fetchall()

for row in pattern_result:
    print(f'{row[0]}: {row[1]} records, {row[2]} unique hashes')
    print(f'  Example: {row[3]}')

# If we have mock data, show the pattern
print('\n2. Mock data transaction hash structure:')
mock_analysis = conn.execute("""
    SELECT 
        transaction_hash,
        COUNT(*) as record_count,
        COUNT(DISTINCT wallet_address) as unique_wallets,
        COUNT(DISTINCT token_address) as unique_tokens,
        STRING_AGG(DISTINCT transaction_type, ', ') as transaction_types
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    WHERE transaction_hash LIKE 'mock_tx_%'
    GROUP BY transaction_hash
    ORDER BY record_count DESC
    LIMIT 5
""").fetchall()

print('Top transaction hashes by record count:')
for row in mock_analysis:
    print(f'Hash: {row[0]}')
    print(f'  Records: {row[1]}, Wallets: {row[2]}, Tokens: {row[3]}, Types: {row[4]}')

# Understand the relationship between transaction hash and records
print('\n3. Key question: Does one blockchain transaction create multiple rows?')

# Look at a specific transaction hash and all its records
if mock_analysis:
    sample_hash = mock_analysis[0][0]
    print(f'\nDetailed analysis of transaction hash: {sample_hash}')
    
    detailed_analysis = conn.execute(f"""
        SELECT 
            wallet_address,
            token_address,
            transaction_type,
            from_symbol,
            to_symbol,
            from_amount,
            to_amount,
            value_usd,
            timestamp
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
        WHERE transaction_hash = '{sample_hash}'
        ORDER BY wallet_address, token_address
    """).fetchall()
    
    print(f'This transaction hash has {len(detailed_analysis)} records:')
    for i, row in enumerate(detailed_analysis):
        print(f'  Record {i+1}:')
        print(f'    Wallet: {row[0][:10]}...')
        print(f'    Token: {row[1][:10]}...')
        print(f'    Type: {row[2]}')
        print(f'    Trade: {row[3]} ({row[5]}) -> {row[4]} ({row[6]})')
        print(f'    Value: ${row[7]}, Time: {row[8]}')

# Check if same wallet appears multiple times for same hash
print('\n4. Do multiple rows exist for the same wallet + transaction_hash combination?')
wallet_hash_analysis = conn.execute("""
    SELECT 
        transaction_hash,
        wallet_address,
        COUNT(*) as records_per_wallet,
        COUNT(DISTINCT token_address) as tokens_per_wallet,
        STRING_AGG(DISTINCT token_address, ', ') as token_list
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    GROUP BY transaction_hash, wallet_address
    HAVING COUNT(*) > 1
    ORDER BY records_per_wallet DESC
    LIMIT 5
""").fetchall()

if wallet_hash_analysis:
    print('Same wallet appearing multiple times for same transaction hash:')
    for row in wallet_hash_analysis:
        print(f'Hash: {row[0]}, Wallet: {row[1][:10]}...')
        print(f'  Records: {row[2]}, Tokens: {row[3]}')
        print(f'  Token list: {row[4][:100]}...' if len(row[4]) > 100 else f'  Token list: {row[4]}')
else:
    print('No wallet appears multiple times for the same transaction hash')

print('\n5. Summary of findings:')
print('Based on the current mock data, it appears that:')
if mock_analysis and mock_analysis[0][1] > 1:
    print('- One blockchain transaction (identified by transaction_hash) can create MULTIPLE rows in our bronze table')
    print('- Each row represents the transaction from the perspective of a different target token')
    print('- The same wallet can have multiple rows for the same transaction_hash when tracking multiple tokens')
    print('- transaction_hash is the Solana blockchain transaction identifier')
    print('- Our bronze table is denormalized by wallet_address + token_address combination')
else:
    print('- Each transaction_hash appears to be unique to one record')
    print('- Need more data to understand the relationship structure')