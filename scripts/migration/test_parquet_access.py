#!/usr/bin/env python3
"""
Test script to verify parquet file access and schema inspection
"""

import duckdb
import json
from datetime import datetime

def test_parquet_access():
    """Test reading parquet files using DuckDB"""
    
    # Initialize DuckDB with S3 configuration
    conn = duckdb.connect()
    
    # Configure S3 settings for MinIO
    conn.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    
    print("Testing parquet file access...")
    
    # Test old schema access
    print("\n1. Testing old schema files:")
    try:
        result = conn.execute("""
            SELECT COUNT(*) as file_count, 
                   COUNT(DISTINCT wallet_address) as unique_wallets,
                   MIN(timestamp) as earliest_transaction,
                   MAX(timestamp) as latest_transaction
            FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
        """).fetchall()
        print(f"Old schema - Files accessible: {result[0][0]} records, {result[0][1]} wallets")
        print(f"Time range: {result[0][2]} to {result[0][3]}")
        
        # Get schema
        schema_result = conn.execute("""
            DESCRIBE SELECT * FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet') LIMIT 1
        """).fetchall()
        print(f"Old schema columns: {len(schema_result)}")
        for col in schema_result[:5]:  # Show first 5 columns
            print(f"  - {col[0]}: {col[1]}")
        
    except Exception as e:
        print(f"Error accessing old schema: {e}")
    
    # Test current schema access
    print("\n2. Testing current schema files:")
    try:
        result = conn.execute("""
            SELECT COUNT(*) as file_count,
                   COUNT(DISTINCT wallet_address) as unique_wallets,
                   MIN(timestamp) as earliest_transaction,
                   MAX(timestamp) as latest_transaction
            FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/wallet_transactions_*.parquet')
        """).fetchall()
        print(f"Current schema - Files accessible: {result[0][0]} records, {result[0][1]} wallets")
        print(f"Time range: {result[0][2]} to {result[0][3]}")
        
        # Get schema
        schema_result = conn.execute("""
            DESCRIBE SELECT * FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/wallet_transactions_*.parquet') LIMIT 1
        """).fetchall()
        print(f"Current schema columns: {len(schema_result)}")
        for col in schema_result[:5]:  # Show first 5 columns
            print(f"  - {col[0]}: {col[1]}")
        
    except Exception as e:
        print(f"Error accessing current schema: {e}")
    
    # Test sample data from each
    print("\n3. Sample data comparison:")
    try:
        print("Old schema sample:")
        old_sample = conn.execute("""
            SELECT wallet_address, from_symbol, to_symbol, from_amount, to_amount, transaction_type, value_usd
            FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
            LIMIT 3
        """).fetchall()
        for row in old_sample:
            print(f"  {row}")
            
    except Exception as e:
        print(f"Error getting old schema sample: {e}")
    
    try:
        print("Current schema sample:")
        current_sample = conn.execute("""
            SELECT wallet_address, base_symbol, quote_symbol, base_ui_amount, quote_ui_amount, base_type_swap, quote_type_swap
            FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/wallet_transactions_*.parquet')
            LIMIT 3
        """).fetchall()
        for row in current_sample:
            print(f"  {row}")
            
    except Exception as e:
        print(f"Error getting current schema sample: {e}")
    
    conn.close()

if __name__ == "__main__":
    test_parquet_access()