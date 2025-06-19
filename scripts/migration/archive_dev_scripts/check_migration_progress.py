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

print("Checking bronze migration progress...")

try:
    stats = conn.execute("""
        SELECT 
            COUNT(*) as migrated_records,
            COUNT(DISTINCT wallet_address) as unique_wallets,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            COUNT(DISTINCT partition_date) as unique_dates
        FROM read_parquet('s3://solana-data/bronze/wallet_trade_history_bronze_fixed/**/*.parquet')
    """).fetchone()
    
    print(f"\n📊 MIGRATION PROGRESS:")
    print(f"  Migrated Records: {stats[0]:,}")
    print(f"  Expected Records: 3,620,060")
    print(f"  Progress: {stats[0]/3620060*100:.1f}%")
    print(f"  Unique Wallets: {stats[1]:,}")
    print(f"  Date Range: {stats[2]} to {stats[3]}")
    print(f"  Unique Dates: {stats[4]:,}")
    
    if stats[0] < 3620060:
        print(f"\n⚠️  INCOMPLETE MIGRATION")
        print(f"  Missing Records: {3620060 - stats[0]:,}")
        print(f"  Need to resume migration from ID: 50000")
    else:
        print(f"\n✅ MIGRATION COMPLETE")

except Exception as e:
    print(f"❌ Error: {e}")

conn.close()
