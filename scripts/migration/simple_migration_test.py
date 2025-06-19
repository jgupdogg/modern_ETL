#!/usr/bin/env python3
import duckdb

conn = duckdb.connect()
conn.execute("""
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin123';
    SET s3_use_ssl=false; SET s3_url_style='path';
""")

print("Creating unified wallet transactions dataset...")

# Simple unified query with UNION ALL
unified_query = """
COPY (
    -- Old schema data (from_symbol/to_symbol format)
    SELECT 
        wallet_address,
        transaction_hash,
        timestamp,
        from_symbol as token_a,
        to_symbol as token_b, 
        from_amount as amount_a,
        to_amount as amount_b,
        COALESCE(value_usd, from_amount * COALESCE(nearest_price, 1.0)) as value_usd,
        COALESCE(transaction_type, 'UNKNOWN') as transaction_type,
        'old_schema' as schema_source,
        DATE(timestamp) as partition_date
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
    
    UNION ALL
    
    -- Current schema v1 (legacy format)
    SELECT 
        wallet_address,
        transaction_hash, 
        timestamp,
        from_symbol as token_a,
        to_symbol as token_b,
        from_amount as amount_a,
        to_amount as amount_b,
        COALESCE(value_usd, from_amount * COALESCE(nearest_price, 1.0)) as value_usd,
        COALESCE(transaction_type, 'UNKNOWN') as transaction_type,
        'current_v1' as schema_source,
        DATE(timestamp) as partition_date
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/date=2025-06-13/wallet_transactions_*.parquet', union_by_name=true)
    
    UNION ALL
    
    -- Current schema v2 (new format base_symbol/quote_symbol)
    SELECT 
        wallet_address,
        transaction_hash,
        timestamp,
        base_symbol as token_a,
        quote_symbol as token_b,
        base_ui_amount as amount_a,
        quote_ui_amount as amount_b,
        COALESCE(base_ui_amount * base_nearest_price, quote_ui_amount * quote_nearest_price, 0) as value_usd,
        CASE 
            WHEN base_type_swap = 'out' AND quote_type_swap = 'in' THEN 'SELL'
            WHEN base_type_swap = 'in' AND quote_type_swap = 'out' THEN 'BUY'
            ELSE 'SWAP'
        END as transaction_type,
        'current_v2' as schema_source,
        DATE(timestamp) as partition_date
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions/date=2025-06-18/wallet_transactions_*.parquet', union_by_name=true)

) TO 's3://solana-data/bronze/wallet_transactions_unified/' (
    FORMAT PARQUET,
    PARTITION_BY partition_date,
    OVERWRITE_OR_IGNORE true
)
"""

try:
    conn.execute(unified_query)
    print("✅ Unified dataset created successfully!")
    
    # Get statistics
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT wallet_address) as unique_wallets,
            COUNT(CASE WHEN schema_source = 'old_schema' THEN 1 END) as old_records,
            COUNT(CASE WHEN schema_source = 'current_v1' THEN 1 END) as v1_records,
            COUNT(CASE WHEN schema_source = 'current_v2' THEN 1 END) as v2_records,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            AVG(value_usd) as avg_value
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions_unified/**/*.parquet')
    """).fetchone()
    
    print(f"\nUnified Dataset Statistics:")
    print(f"  Total Records: {stats[0]:,}")
    print(f"  Unique Wallets: {stats[1]:,}")
    print(f"  Old Schema: {stats[2]:,} records")
    print(f"  Current v1: {stats[3]:,} records") 
    print(f"  Current v2: {stats[4]:,} records")
    print(f"  Time Range: {stats[5]} to {stats[6]}")
    print(f"  Avg Value: ${stats[7]:.2f}")
    
    print(f"\n🎯 Ready for comprehensive smart trader analysis!")
    print(f"📂 Location: s3://solana-data/bronze/wallet_transactions_unified/")
    
except Exception as e:
    print(f"❌ Error: {e}")

conn.close()