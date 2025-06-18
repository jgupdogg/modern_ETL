#!/usr/bin/env python3
"""
DuckDB Initialization Script
Sets up DuckDB with S3/MinIO configuration and required extensions
"""

import duckdb
import os
import time

def initialize_duckdb():
    """Initialize DuckDB with proper configuration"""
    try:
        print("🔧 Initializing DuckDB...")
        
        # Connect to DuckDB
        conn = duckdb.connect('/data/analytics.duckdb')
        
        # Load required extensions
        print("📦 Loading DuckDB extensions...")
        conn.execute('INSTALL httpfs;')
        conn.execute('LOAD httpfs;')
        
        # Configure S3/MinIO settings
        print("🔗 Configuring MinIO connection...")
        conn.execute("SET s3_endpoint='minio:9000';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin123';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        # Test connection
        print("✅ Testing MinIO connection...")
        try:
            result = conn.execute("SELECT COUNT(*) FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet');").fetchone()
            print(f"📊 Found {result[0]} wallet PnL records in silver layer")
        except Exception as e:
            print(f"⚠️  MinIO test failed (expected if no data): {e}")
        
        # Close connection
        conn.close()
        print("🎉 DuckDB initialization completed successfully!")
        
        return True
        
    except Exception as e:
        print(f"❌ DuckDB initialization failed: {e}")
        return False

if __name__ == "__main__":
    # Keep trying until successful
    max_retries = 10
    for attempt in range(max_retries):
        if initialize_duckdb():
            break
        else:
            print(f"Retrying in 5 seconds... (attempt {attempt + 1}/{max_retries})")
            time.sleep(5)
    
    # Keep container running
    print("🔄 DuckDB service ready, keeping container alive...")
    while True:
        time.sleep(60)