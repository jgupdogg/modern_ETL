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

print("Analyzing USD value data in unified dataset vs original schemas...")

# Check unified dataset USD values
print("\n1. UNIFIED DATASET USD VALUES:")
unified_usd = conn.execute("""
    SELECT 
        schema_source,
        COUNT(*) as total_records,
        COUNT(CASE WHEN value_usd > 0 THEN 1 END) as records_with_usd,
        COUNT(CASE WHEN value_usd = 0 THEN 1 END) as records_zero_usd,
        AVG(CASE WHEN value_usd > 0 THEN value_usd END) as avg_nonzero_usd,
        MAX(value_usd) as max_usd,
        MIN(CASE WHEN value_usd > 0 THEN value_usd END) as min_nonzero_usd
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_unified/**/*.parquet')
    GROUP BY schema_source
    ORDER BY schema_source
""").fetchall()

for row in unified_usd:
    source, total, with_usd, zero_usd, avg_usd, max_usd, min_usd = row
    pct_with_usd = (with_usd / total * 100) if total > 0 else 0
    print(f"\n  {source.upper()}:")
    print(f"    Total Records: {total:,}")
    print(f"    With USD Values: {with_usd:,} ({pct_with_usd:.1f}%)")
    print(f"    Zero USD Values: {zero_usd:,}")
    print(f"    Avg USD (non-zero): ${avg_usd:.2f}" if avg_usd else "    Avg USD: N/A")
    print(f"    Range: ${min_usd:.2f} - ${max_usd:.2f}" if min_usd else "    Range: N/A")

# Check what's available in original current schema
print("\n2. ORIGINAL CURRENT SCHEMA PRICE FIELDS:")
try:
    current_schema_fields = conn.execute("""
        DESCRIBE SELECT * FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/wallet_transactions_*.parquet', union_by_name=true) LIMIT 1
    """).fetchall()
    
    price_fields = [col[0] for col in current_schema_fields if any(keyword in col[0].lower() for keyword in ['price', 'value', 'usd'])]
    print(f"  Available price/value fields: {price_fields}")
    
    # Sample the price data availability
    price_sample = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN base_nearest_price > 0 THEN 1 END) as base_price_available,
            COUNT(CASE WHEN quote_nearest_price > 0 THEN 1 END) as quote_price_available,
            AVG(CASE WHEN base_nearest_price > 0 THEN base_nearest_price END) as avg_base_price,
            AVG(CASE WHEN quote_nearest_price > 0 THEN quote_nearest_price END) as avg_quote_price,
            MAX(base_nearest_price) as max_base_price,
            MAX(quote_nearest_price) as max_quote_price
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/wallet_transactions_*.parquet', union_by_name=true)
        WHERE base_symbol IS NOT NULL
    """).fetchone()
    
    total, base_avail, quote_avail, avg_base, avg_quote, max_base, max_quote = price_sample
    print(f"\n  CURRENT SCHEMA PRICE DATA:")
    print(f"    Total Records: {total:,}")
    print(f"    Base Price Available: {base_avail:,} ({base_avail/total*100:.1f}%)")
    print(f"    Quote Price Available: {quote_avail:,} ({quote_avail/total*100:.1f}%)")
    print(f"    Avg Base Price: ${avg_base:.6f}" if avg_base else "    Avg Base Price: N/A")
    print(f"    Avg Quote Price: ${avg_quote:.6f}" if avg_quote else "    Avg Quote Price: N/A")
    print(f"    Max Base Price: ${max_base:.2f}" if max_base else "    Max Base Price: N/A")
    print(f"    Max Quote Price: ${max_quote:.2f}" if max_quote else "    Max Quote Price: N/A")

except Exception as e:
    print(f"  Error analyzing current schema: {e}")

# Check old schema USD values
print("\n3. ORIGINAL OLD SCHEMA USD VALUES:")
try:
    old_usd = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN value_usd > 0 THEN 1 END) as with_usd,
            AVG(CASE WHEN value_usd > 0 THEN value_usd END) as avg_usd,
            MAX(value_usd) as max_usd,
            MIN(CASE WHEN value_usd > 0 THEN value_usd END) as min_usd
        FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
    """).fetchone()
    
    total, with_usd, avg_usd, max_usd, min_usd = old_usd
    print(f"  Total Records: {total:,}")
    print(f"  With USD Values: {with_usd:,} ({with_usd/total*100:.1f}%)")
    print(f"  Avg USD: ${avg_usd:.2f}" if avg_usd else "  Avg USD: N/A")
    print(f"  Range: ${min_usd:.2f} - ${max_usd:.2f}" if min_usd else "  Range: N/A")

except Exception as e:
    print(f"  Error analyzing old schema: {e}")

# Check if we need to improve USD calculation for unified dataset
print("\n4. RECOMMENDATIONS:")
print("  Based on price field availability, we can improve USD calculations by:")

# Show sample transactions with missing USD values
missing_usd_sample = conn.execute("""
    SELECT 
        schema_source,
        SUBSTR(wallet_address, 1, 8) || '...' as wallet,
        token_a || ' -> ' || token_b as trade,
        amount_a,
        value_usd,
        DATE(timestamp) as trade_date
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_unified/**/*.parquet')
    WHERE value_usd = 0
    ORDER BY amount_a DESC
    LIMIT 5
""").fetchall()

print(f"\n5. SAMPLE TRANSACTIONS WITH MISSING USD VALUES:")
for row in missing_usd_sample:
    source, wallet, trade, amount, usd, date = row
    print(f"  {wallet}  < /dev/null |  {trade} | {amount:.4f} tokens | ${usd:.2f} | {source} | {date}")

conn.close()
