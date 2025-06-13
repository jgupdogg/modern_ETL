#!/usr/bin/env python3
"""
Check all unique wallet addresses in gold layer.
"""

import duckdb

def main():
    print("🔍 CHECKING GOLD LAYER ADDRESSES")
    print("=" * 50)
    
    conn = duckdb.connect("/data/analytics.duckdb")
    try:
        # Configure S3/MinIO access
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_endpoint='minio:9000';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin123';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        # Count unique addresses
        count_query = """
        SELECT COUNT(DISTINCT wallet_address) 
        FROM read_parquet('s3://solana-data/gold/top_traders/*.parquet') 
        WHERE wallet_address IS NOT NULL AND wallet_address != ''
        """
        
        result = conn.execute(count_query).fetchall()
        total_count = result[0][0]
        print(f"📊 Total unique addresses: {total_count}")
        
        # Get all addresses
        addresses_query = """
        SELECT DISTINCT wallet_address 
        FROM read_parquet('s3://solana-data/gold/top_traders/*.parquet') 
        WHERE wallet_address IS NOT NULL AND wallet_address != ''
        ORDER BY wallet_address
        """
        
        result = conn.execute(addresses_query).fetchall()
        addresses = [row[0] for row in result]
        
        print(f"\n📋 ALL {len(addresses)} ADDRESSES:")
        for i, addr in enumerate(addresses, 1):
            print(f"  {i:2d}. {addr}")
        
        print(f"\n✅ Successfully retrieved {len(addresses)} unique wallet addresses")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()