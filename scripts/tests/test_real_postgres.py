#!/usr/bin/env python3
"""
Test connection to the real PostgreSQL database with Solana data.
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/home/jgupdogg/dev/claude_pipeline/.env')

def test_postgres_connection():
    """Test connection and check gold.top_traders table."""
    print("🔍 TESTING REAL POSTGRESQL CONNECTION")
    print("=" * 50)
    
    pg_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'solana_pipeline'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'St0ck!adePG')
    }
    
    print(f"📡 Connecting to: {pg_config['user']}@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}")
    
    try:
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor() as cursor:
                print("✅ Connection successful!")
                
                # Check if gold schema exists
                cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold'")
                gold_schema = cursor.fetchone()
                
                if gold_schema:
                    print("✅ Gold schema exists")
                    
                    # Check if top_traders table exists
                    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold' AND table_name = 'top_traders'")
                    table_exists = cursor.fetchone()
                    
                    if table_exists:
                        print("✅ gold.top_traders table exists")
                        
                        # Get table info
                        cursor.execute("SELECT COUNT(*) FROM gold.top_traders")
                        total_records = cursor.fetchone()[0]
                        print(f"📊 Total records: {total_records}")
                        
                        cursor.execute("SELECT COUNT(DISTINCT wallet_address) FROM gold.top_traders WHERE wallet_address IS NOT NULL")
                        unique_addresses = cursor.fetchone()[0]
                        print(f"📍 Unique addresses: {unique_addresses}")
                        
                        # Show sample data
                        cursor.execute("SELECT * FROM gold.top_traders LIMIT 5")
                        sample_data = cursor.fetchall()
                        
                        # Get column names
                        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'gold' AND table_name = 'top_traders' ORDER BY ordinal_position")
                        columns = [row[0] for row in cursor.fetchall()]
                        
                        print(f"\n🏷️  Columns: {columns}")
                        print(f"📋 SAMPLE DATA (first 5 records):")
                        for i, row in enumerate(sample_data, 1):
                            data_dict = dict(zip(columns, row))
                            print(f"  {i}. ID: {data_dict.get('id')}, Wallet: {data_dict.get('wallet_address', 'N/A')[:20]}..., Rank: {data_dict.get('rank')}")
                        
                        return True
                    else:
                        print("❌ gold.top_traders table not found")
                        
                        # List available tables in gold schema
                        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold'")
                        gold_tables = cursor.fetchall()
                        print(f"📋 Available tables in gold schema: {[t[0] for t in gold_tables]}")
                else:
                    print("❌ Gold schema not found")
                    
                    # List available schemas
                    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')")
                    schemas = cursor.fetchall()
                    print(f"📋 Available schemas: {[s[0] for s in schemas]}")
                    
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    
    return False

if __name__ == "__main__":
    test_postgres_connection()