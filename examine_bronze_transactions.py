#!/usr/bin/env python3
"""
Examine bronze wallet transactions to understand the data structure
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

print('=== Bronze Wallet Transactions Data Structure Analysis ===')

# 1. Check if any data exists
print('\n1. Check if bronze transaction data exists:')
count_result = conn.execute("""
    SELECT COUNT(*) as total_count
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
""").fetchall()

if count_result and count_result[0][0] > 0:
    print(f'Total transactions: {count_result[0][0]}')
    
    # 2. Show a few sample records with key fields
    print('\n2. Sample transaction records (showing key structure fields):')
    sample_result = conn.execute("""
        SELECT 
            wallet_address,
            token_address,
            transaction_hash,
            transaction_type,
            timestamp,
            from_symbol,
            to_symbol,
            from_amount,
            to_amount,
            value_usd,
            block_unix_time
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
        LIMIT 5
    """).fetchall()
    
    for i, row in enumerate(sample_result):
        print(f'\nTransaction {i+1}:')
        print(f'  Wallet: {row[0][:10]}...')
        print(f'  Token: {row[1][:10]}...')
        print(f'  Transaction Hash: {row[2]}')
        print(f'  Type: {row[3]}')
        print(f'  Timestamp: {row[4]}')
        print(f'  From: {row[5]} ({row[7]}) -> To: {row[6]} ({row[8]})')
        print(f'  Value USD: {row[9]}')
        print(f'  Block Unix Time: {row[10]}')
    
    # 3. Check transaction hash uniqueness
    print('\n3. Transaction hash analysis:')
    hash_analysis = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT transaction_hash) as unique_hashes,
            COUNT(*) - COUNT(DISTINCT transaction_hash) as duplicate_hashes
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
    """).fetchall()
    
    row = hash_analysis[0]
    print(f'Total records: {row[0]}')
    print(f'Unique transaction hashes: {row[1]}')
    print(f'Duplicate hashes: {row[2]}')
    print(f'Ratio: {row[1]/row[0]*100:.1f}% unique hashes')
    
    # 4. Show examples of records with same transaction hash (if any)
    if row[2] > 0:
        print('\n4. Examples of records sharing the same transaction hash:')
        duplicate_result = conn.execute("""
            SELECT 
                transaction_hash,
                COUNT(*) as record_count,
                STRING_AGG(DISTINCT wallet_address, ', ') as wallets,
                STRING_AGG(DISTINCT token_address, ', ') as tokens,
                STRING_AGG(DISTINCT transaction_type, ', ') as types
            FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
            GROUP BY transaction_hash
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 3
        """).fetchall()
        
        for row in duplicate_result:
            print(f'Hash: {row[0]}')
            print(f'  Records: {row[1]}')
            print(f'  Wallets: {row[2][:50]}...' if len(row[2]) > 50 else f'  Wallets: {row[2]}')
            print(f'  Tokens: {row[3][:50]}...' if len(row[3]) > 50 else f'  Tokens: {row[3]}')
            print(f'  Types: {row[4]}')
            print()
        
        # 5. Look at specific multi-record transaction
        print('\n5. Detailed view of a transaction with multiple records:')
        first_hash = duplicate_result[0][0]
        detailed_result = conn.execute(f"""
            SELECT 
                wallet_address,
                token_address,
                transaction_type,
                from_symbol,
                to_symbol,
                from_amount,
                to_amount,
                value_usd
            FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
            WHERE transaction_hash = '{first_hash}'
            ORDER BY wallet_address, token_address
        """).fetchall()
        
        print(f'Transaction Hash: {first_hash}')
        for i, row in enumerate(detailed_result):
            print(f'  Record {i+1}:')
            print(f'    Wallet: {row[0][:10]}...')
            print(f'    Token: {row[1][:10]}...')
            print(f'    Type: {row[2]}')
            print(f'    {row[3]} ({row[5]}) -> {row[4]} ({row[6]})')
            print(f'    Value: ${row[7]}')
    else:
        print('\n4. No duplicate transaction hashes found - each hash is unique per record')
    
    # 6. Check transaction type distribution
    print('\n6. Transaction type distribution:')
    type_dist = conn.execute("""
        SELECT 
            transaction_type,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
        GROUP BY transaction_type
        ORDER BY count DESC
    """).fetchall()
    
    for row in type_dist:
        print(f'{row[0]}: {row[1]} ({row[2]}%)')

else:
    print('No bronze transaction data found in MinIO')
    print('This means the bronze wallet transactions DAG has not run successfully yet')