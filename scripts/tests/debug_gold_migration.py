#!/usr/bin/env python3
"""
Debug the gold.top_traders migration issue.
"""

import duckdb
import pandas as pd

def check_minio_data():
    """Check what's actually in MinIO."""
    print("🔍 DEBUGGING GOLD.TOP_TRADERS MIGRATION")
    print("=" * 60)
    
    conn = duckdb.connect("/data/analytics.duckdb")
    
    try:
        # Configure S3/MinIO access
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_endpoint='minio:9000';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin123';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        # Check what's in the parquet file
        print("📊 WHAT'S IN MINIO:")
        result = conn.execute("SELECT * FROM read_parquet('s3://solana-data/gold/top_traders/*.parquet')").fetchall()
        
        if result:
            # Get column names
            columns = [desc[0] for desc in conn.description]
            print(f"🏷️  Columns: {columns}")
            print(f"📈 Total records: {len(result)}")
            
            print(f"\n📋 ALL DATA:")
            for i, row in enumerate(result, 1):
                print(f"  {i}. {dict(zip(columns, row))}")
        else:
            print("❌ No data found in MinIO")
            
        # Check unique wallet addresses
        result = conn.execute("SELECT DISTINCT wallet_address FROM read_parquet('s3://solana-data/gold/top_traders/*.parquet') WHERE wallet_address IS NOT NULL").fetchall()
        unique_addresses = [row[0] for row in result if row[0]]
        
        print(f"\n📍 UNIQUE ADDRESSES ({len(unique_addresses)}):")
        for addr in unique_addresses:
            print(f"  • {addr}")
            
    except Exception as e:
        print(f"❌ Error reading from MinIO: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def check_postgres_data():
    """Check what should be in PostgreSQL."""
    print(f"\n🗄️  CHECKING POSTGRESQL DATA:")
    print("-" * 40)
    
    try:
        import psycopg2
        import os
        
        # Use the same config as migration script
        pg_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'solana_tracker'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'password')
        }
        
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor() as cursor:
                # Check total records
                cursor.execute("SELECT COUNT(*) FROM gold.top_traders")
                total_records = cursor.fetchone()[0]
                print(f"📊 Total records in PostgreSQL: {total_records}")
                
                # Check unique addresses
                cursor.execute("SELECT COUNT(DISTINCT wallet_address) FROM gold.top_traders WHERE wallet_address IS NOT NULL")
                unique_addresses = cursor.fetchone()[0]
                print(f"📍 Unique addresses in PostgreSQL: {unique_addresses}")
                
                # Show sample data
                cursor.execute("SELECT * FROM gold.top_traders LIMIT 5")
                sample_data = cursor.fetchall()
                
                # Get column names
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'gold' AND table_name = 'top_traders' ORDER BY ordinal_position")
                columns = [row[0] for row in cursor.fetchall()]
                
                print(f"\n🏷️  Columns: {columns}")
                print(f"📋 SAMPLE DATA (first 5 records):")
                for i, row in enumerate(sample_data, 1):
                    print(f"  {i}. {dict(zip(columns, row))}")
                
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")

if __name__ == "__main__":
    check_minio_data()
    check_postgres_data()