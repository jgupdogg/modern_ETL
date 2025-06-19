#\!/usr/bin/env python3
import duckdb

conn = duckdb.connect()
conn.execute("""
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin123';
    SET s3_use_ssl=false; SET s3_url_style='path';
""")

print("Validating PostgreSQL wallet_trade_history migration...")

# Check migrated data
try:
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT wallet_address) as unique_wallets,
            COUNT(DISTINCT token_a) as unique_tokens_a,
            COUNT(DISTINCT token_b) as unique_tokens_b,
            COUNT(CASE WHEN value_usd > 0 THEN 1 END) as records_with_usd,
            AVG(CASE WHEN value_usd > 0 THEN value_usd END) as avg_usd_value,
            MAX(value_usd) as max_usd_value,
            MIN(timestamp) as earliest_record,
            MAX(timestamp) as latest_record,
            COUNT(DISTINCT transaction_type) as unique_tx_types
        FROM read_parquet('s3://solana-data/bronze/wallet_trade_history_postgres/**/*.parquet')
    """).fetchone()
    
    print(f"\n✅ MIGRATION VALIDATION SUCCESSFUL")
    print(f"  Total Records: {stats[0]:,}")
    print(f"  Unique Wallets: {stats[1]:,}")
    print(f"  Unique Token A: {stats[2]:,}")
    print(f"  Unique Token B: {stats[3]:,}")
    print(f"  Records with USD: {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)")
    print(f"  Avg USD Value: ${stats[5]:.2f}" if stats[5] else "  Avg USD: N/A")
    print(f"  Max USD Value: ${stats[6]:.2f}" if stats[6] else "  Max USD: N/A")
    print(f"  Date Range: {stats[7]} to {stats[8]}")
    print(f"  Transaction Types: {stats[9]}")
    
    # Check schema compatibility with unified format
    schema_check = conn.execute("""
        DESCRIBE SELECT * FROM read_parquet('s3://solana-data/bronze/wallet_trade_history_postgres/**/*.parquet') LIMIT 1
    """).fetchall()
    
    expected_fields = ['wallet_address', 'transaction_hash', 'timestamp', 'token_a', 'token_b', 
                      'amount_a', 'amount_b', 'value_usd', 'transaction_type', 'base_price', 'quote_price']
    
    actual_fields = [col[0] for col in schema_check]
    missing_fields = [field for field in expected_fields if field not in actual_fields]
    
    print(f"\n🔍 SCHEMA COMPATIBILITY:")
    print(f"  Total Fields: {len(actual_fields)}")
    print(f"  Expected Fields Present: {len(expected_fields) - len(missing_fields)}/{len(expected_fields)}")
    if missing_fields:
        print(f"  Missing Fields: {missing_fields}")
    else:
        print(f"  ✅ All expected fields present\!")
    
    # Sample data
    sample = conn.execute("""
        SELECT wallet_address, token_a, token_b, value_usd, transaction_type, blockchain
        FROM read_parquet('s3://solana-data/bronze/wallet_trade_history_postgres/**/*.parquet')
        ORDER BY value_usd DESC
        LIMIT 3
    """).fetchall()
    
    print(f"\n📝 SAMPLE MIGRATED DATA:")
    for row in sample:
        wallet, token_a, token_b, usd, tx_type, blockchain = row
        print(f"  {wallet[:8]}...  < /dev/null |  {token_a} -> {token_b} | ${usd:.2f} | {tx_type} | {blockchain}")

except Exception as e:
    print(f"❌ Validation Error: {e}")

conn.close()
