#!/usr/bin/env python3
"""
Test script for silver tracked tokens transformation
Uses DuckDB container directly to test the logic
"""
import subprocess
import json
import sys

def test_silver_transformation():
    """Test the silver transformation logic"""
    
    # Create DuckDB script
    duckdb_script = '''
import duckdb
import json
import sys

# Connect to DuckDB
conn = duckdb.connect("/data/analytics.duckdb")

try:
    # Configure S3/MinIO access
    conn.execute("LOAD httpfs;")
    conn.execute("SET s3_endpoint='minio:9000';")
    conn.execute("SET s3_access_key_id='minioadmin';")
    conn.execute("SET s3_secret_access_key='minioadmin123';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    # First check what bronze data is available
    bronze_query = """
    SELECT COUNT(*) as total_tokens
    FROM read_parquet('s3://solana-data/bronze/token_list_v3/**/*.parquet')
    """
    
    result = conn.execute(bronze_query).fetchone()
    total_bronze_tokens = result[0] if result else 0
    print(f"Found {total_bronze_tokens} tokens in bronze layer")
    
    if total_bronze_tokens == 0:
        print("NO_BRONZE_DATA")
        sys.exit(0)
    
    # Simple test query to see what data looks like
    sample_query = """
    SELECT 
        token_address,
        symbol,
        liquidity,
        volume_24h_usd,
        price_change_24h_percent,
        logo_uri
    FROM read_parquet('s3://solana-data/bronze/token_list_v3/**/*.parquet')
    WHERE token_address IS NOT NULL
    LIMIT 5
    """
    
    result = conn.execute(sample_query).fetchall()
    columns = [desc[0] for desc in conn.description]
    
    print("SAMPLE_DATA_START")
    print(json.dumps(columns))
    for row in result:
        print(json.dumps(list(row)))
    print("SAMPLE_DATA_END")
    
    # Now test filtering logic
    filter_query = """
    WITH bronze_tokens AS (
        SELECT 
            token_address,
            symbol,
            name,
            decimals,
            logo_uri,
            liquidity,
            price,
            fdv,
            market_cap,
            volume_24h_usd,
            volume_24h_change_percent,
            price_change_1h_percent,
            price_change_2h_percent,
            price_change_4h_percent,
            price_change_8h_percent,
            price_change_24h_percent,
            ingested_at,
            batch_id,
            -- Add row number to handle duplicates
            ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ingested_at DESC) as rn
        FROM read_parquet('s3://solana-data/bronze/token_list_v3/**/*.parquet')
        WHERE token_address IS NOT NULL
    ),
    filtered_tokens AS (
        SELECT *
        FROM bronze_tokens
        WHERE rn = 1  -- Keep only latest version of each token
          AND logo_uri IS NOT NULL 
          AND logo_uri != ''
          AND liquidity >= 10000
          AND volume_24h_usd >= 50000
          AND price_change_24h_percent >= 30
          -- All price changes must be positive
          AND price_change_1h_percent > 0
          AND price_change_2h_percent > 0
          AND price_change_4h_percent > 0
          AND price_change_8h_percent > 0
          AND price_change_24h_percent > 0
    )
    SELECT COUNT(*) as filtered_count
    FROM filtered_tokens
    """
    
    result = conn.execute(filter_query).fetchone()
    filtered_count = result[0] if result else 0
    
    print(f"FILTERED_COUNT: {filtered_count}")
    
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
finally:
    conn.close()
'''
    
    print("Testing silver transformation logic...")
    
    # Execute DuckDB script in container
    cmd = ['docker', 'exec', 'claude_pipeline-duckdb', 'python3', '-c', duckdb_script]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    
    if result.returncode != 0:
        print(f"❌ Test failed: {result.stderr}")
        return False
    
    print("✅ Test output:")
    print(result.stdout)
    
    return True

if __name__ == "__main__":
    success = test_silver_transformation()
    sys.exit(0 if success else 1)