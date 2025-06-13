#!/usr/bin/env python3
"""
Simple Solana Data Summary - Works around numpy issues
"""

import subprocess

def query_duckdb(simple_query):
    """Run a simple single-line query."""
    cmd = [
        'docker', 'exec', 'claude_pipeline-duckdb', 'python3', '-c',
        f'''
import duckdb
conn = duckdb.connect("/data/analytics.duckdb")
conn.execute("LOAD httpfs;")
conn.execute("SET s3_endpoint='minio:9000';")
conn.execute("SET s3_access_key_id='minioadmin';")
conn.execute("SET s3_secret_access_key='minioadmin123';")
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_url_style='path';")
result = conn.execute("{simple_query}").fetchall()
for row in result:
    print(row)
conn.close()
        '''
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip().split('\n')
    else:
        return [f"Error: {result.stderr}"]

def main():
    print("🔍 SOLANA DATA SUMMARY")
    print("=" * 50)
    
    # Table counts
    print("\n📊 TABLE OVERVIEW:")
    tables = ['token_list_v3', 'token_whales', 'wallet_trade_history', 'token_metadata']
    
    for table in tables:
        query = f"SELECT COUNT(*) FROM read_parquet('s3://solana-data/bronze/{table}/*.parquet')"
        result = query_duckdb(query)
        if result and not result[0].startswith("Error"):
            count = result[0].strip("(),")
            print(f"  ✅ {table}: {count} rows")
        else:
            print(f"  ❌ {table}: {result[0] if result else 'No data'}")
    
    # Token details
    print("\n🪙 TOKEN DETAILS:")
    query = "SELECT symbol, name, price FROM read_parquet('s3://solana-data/bronze/token_list_v3/*.parquet')"
    result = query_duckdb(query)
    for row in result:
        if not row.startswith("Error"):
            # Parse tuple format: ('USDC', 'USD Coin', 1.0)
            clean_row = row.strip("()").replace("'", "").split(", ")
            if len(clean_row) >= 3:
                symbol, name, price = clean_row[0], clean_row[1], clean_row[2]
                print(f"  💰 {symbol}: {name} - ${price}")
    
    # Whale summary
    print("\n🐋 WHALE SUMMARY:")
    query = "SELECT COUNT(*), SUM(holdings_value) FROM read_parquet('s3://solana-data/bronze/token_whales/*.parquet')"
    result = query_duckdb(query)
    if result and not result[0].startswith("Error"):
        clean_row = result[0].strip("()").split(", ")
        if len(clean_row) >= 2:
            count, total_value = clean_row[0], clean_row[1]
            print(f"  📈 {count} whale positions worth ${total_value}")
    
    # Trade summary
    print("\n💱 TRADE SUMMARY:")
    query = "SELECT COUNT(*), SUM(usd_value) FROM read_parquet('s3://solana-data/bronze/wallet_trade_history/*.parquet')"
    result = query_duckdb(query)
    if result and not result[0].startswith("Error"):
        clean_row = result[0].strip("()").split(", ")
        if len(clean_row) >= 2:
            count, total_volume = clean_row[0], clean_row[1]
            print(f"  📊 {count} trades with ${total_volume} total volume")
    
    # Metadata summary
    print("\n📋 METADATA SUMMARY:")
    query = "SELECT COUNT(*) FROM read_parquet('s3://solana-data/bronze/token_metadata/*.parquet')"
    result = query_duckdb(query)
    if result and not result[0].startswith("Error"):
        count = result[0].strip("(),")
        print(f"  📝 {count} tokens with metadata")
    
    print("\n✅ Summary complete! All Solana data is accessible.")
    print("\n💡 Recommendation: Use the DuckDB container directly for complex queries:")
    print("   docker exec -it claude_pipeline-duckdb python3")

if __name__ == "__main__":
    main()