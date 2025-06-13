#!/usr/bin/env python3
"""
Simple Solana Data Explorer
Works around numpy import issues by using subprocess to run commands in DuckDB container.
"""

import subprocess
import json
import os

def run_duckdb_query(query):
    """Run a query in the DuckDB container and return results."""
    # Escape quotes properly for shell execution
    query_escaped = query.replace('"', '\\"').replace("'", "\\'")
    
    cmd = f'''
    docker exec claude_pipeline-duckdb python3 -c "
import duckdb
conn = duckdb.connect('/data/analytics.duckdb')
conn.execute('LOAD httpfs;')
conn.execute('SET s3_endpoint=\\"minio:9000\\";')
conn.execute('SET s3_access_key_id=\\"minioadmin\\";')
conn.execute('SET s3_secret_access_key=\\"minioadmin123\\";')
conn.execute('SET s3_use_ssl=false;')
conn.execute('SET s3_url_style=\\"path\\";')

try:
    query_str = \\"{query_escaped}\\"
    result = conn.execute(query_str).fetchall()
    print('RESULT_START')
    for row in result:
        print(row)
    print('RESULT_END')
except Exception as e:
    print(f'ERROR: {{e}}')
finally:
    conn.close()
"
    '''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        # Parse the output between RESULT_START and RESULT_END
        lines = result.stdout.split('\n')
        in_result = False
        data = []
        for line in lines:
            if line == 'RESULT_START':
                in_result = True
                continue
            elif line == 'RESULT_END':
                break
            elif in_result:
                data.append(line)
        return data
    else:
        print(f"Error: {result.stderr}")
        return []

def main():
    """Main exploration function."""
    print("🔍 Solana Data Explorer")
    print("=" * 50)
    
    # Table overview
    print("\\n📊 TABLE OVERVIEW")
    tables = ['token_list_v3', 'token_whales', 'wallet_trade_history', 'token_metadata']
    
    for table in tables:
        query = f"SELECT COUNT(*) FROM read_parquet('s3://solana-data/bronze/{table}/*.parquet')"
        result = run_duckdb_query(query)
        if result:
            count = result[0].strip("(),")
            print(f"  {table}: {count} rows")
    
    # Token list details
    print("\\n🪙 TOKEN LIST")
    query = "SELECT symbol, name, price, market_cap FROM read_parquet('s3://solana-data/bronze/token_list_v3/*.parquet')"
    result = run_duckdb_query(query)
    for row in result:
        # Parse the tuple string
        row_clean = row.strip("()").replace("'", "").split(", ")
        if len(row_clean) >= 4:
            symbol, name, price, market_cap = row_clean[:4]
            print(f"  {symbol}: {name} - Price: ${price}, Market Cap: ${market_cap}")
    
    # Top whale positions
    print("\\n🐋 TOP WHALE POSITIONS")
    query = """
    SELECT w.wallet_address, t.symbol, w.holdings_value, w.holdings_pct
    FROM read_parquet('s3://solana-data/bronze/token_whales/*.parquet') w
    LEFT JOIN read_parquet('s3://solana-data/bronze/token_list_v3/*.parquet') t
        ON w.token_address = t.token_address
    ORDER BY w.holdings_value DESC
    LIMIT 3
    """
    result = run_duckdb_query(query)
    for row in result:
        # Parse the tuple string - handle potential None values
        row_clean = row.strip("()").replace("'", "").replace("None", "N/A")
        parts = [p.strip() for p in row_clean.split(", ")]
        if len(parts) >= 4:
            wallet, symbol, value, pct = parts[:4]
            print(f"  {wallet[:8]}... holds {symbol} worth ${value} ({pct}%)")
    
    # Trading summary
    print("\\n💱 TRADING SUMMARY")
    query = """
    SELECT 
        COUNT(*) as total_trades,
        SUM(usd_value) as total_volume,
        AVG(usd_value) as avg_trade_size
    FROM read_parquet('s3://solana-data/bronze/wallet_trade_history/*.parquet')
    """
    result = run_duckdb_query(query)
    if result:
        row_clean = result[0].strip("()").split(", ")
        if len(row_clean) >= 3:
            trades, volume, avg_size = row_clean[:3]
            print(f"  Total trades: {trades}")
            print(f"  Total volume: ${volume}")
            print(f"  Average trade size: ${avg_size}")
    
    # Token metadata completeness
    print("\\n📋 METADATA COMPLETENESS")
    query = """
    SELECT 
        COUNT(*) as total,
        COUNT(twitter) as has_twitter,
        COUNT(website) as has_website,
        COUNT(description) as has_description
    FROM read_parquet('s3://solana-data/bronze/token_metadata/*.parquet')
    """
    result = run_duckdb_query(query)
    if result:
        row_clean = result[0].strip("()").split(", ")
        if len(row_clean) >= 4:
            total, twitter, website, desc = row_clean[:4]
            print(f"  Total tokens: {total}")
            print(f"  With Twitter: {twitter}/{total}")
            print(f"  With Website: {website}/{total}")
            print(f"  With Description: {desc}/{total}")
    
    print("\\n✅ Exploration complete!")

if __name__ == "__main__":
    main()