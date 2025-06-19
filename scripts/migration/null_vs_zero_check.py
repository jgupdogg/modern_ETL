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

print("Checking NULL vs ZERO values in old schema...")

# Check NULL vs zero values properly
null_zero_check = conn.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN value_usd IS NULL THEN 1 END) as null_usd,
        COUNT(CASE WHEN value_usd = 0 THEN 1 END) as zero_usd,
        COUNT(CASE WHEN value_usd > 0 THEN 1 END) as positive_usd,
        COUNT(CASE WHEN value_usd IS NOT NULL THEN 1 END) as not_null_usd
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
""").fetchone()

total, null_usd, zero_usd, pos_usd, not_null = null_zero_check
print(f"OLD SCHEMA VALUE_USD BREAKDOWN:")
print(f"  Total records: {total:,}")
print(f"  NULL values: {null_usd:,} ({null_usd/total*100:.1f}%)")
print(f"  Zero values: {zero_usd:,} ({zero_usd/total*100:.1f}%)")
print(f"  Positive values: {pos_usd:,} ({pos_usd/total*100:.1f}%)")
print(f"  Not null total: {not_null:,}")

# Sample NULL records
print(f"\nSAMPLE NULL USD RECORDS:")
null_sample = conn.execute("""
    SELECT 
        from_symbol, to_symbol, from_amount, to_amount, 
        base_price, quote_price, value_usd,
        CASE WHEN base_price > 0 THEN from_amount * base_price ELSE NULL END as calc_base,
        CASE WHEN quote_price > 0 THEN to_amount * quote_price ELSE NULL END as calc_quote
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
    WHERE value_usd IS NULL
    LIMIT 5
""").fetchall()

for row in null_sample:
    from_sym, to_sym, from_amt, to_amt, base_p, quote_p, usd_val, calc_base, calc_quote = row
    print(f"  {from_sym} -> {to_sym}  < /dev/null |  from:{from_amt:.4f} to:{to_amt:.4f}")
    print(f"    Prices: base:${base_p:.6f} quote:${quote_p:.6f} | Original USD: {usd_val}")
    print(f"    Could calculate: base=${calc_base:.2f}, quote=${calc_quote:.2f}" if calc_base or calc_quote else "    Cannot calculate")

# Check our unified migration handling of NULL values
print(f"\nUNIFIED MIGRATION NULL HANDLING:")
unified_null_check = conn.execute("""
    SELECT 
        COUNT(*) as total_old_records,
        COUNT(CASE WHEN value_usd IS NULL THEN 1 END) as null_in_unified,
        COUNT(CASE WHEN value_usd = 0 THEN 1 END) as zero_in_unified,
        COUNT(CASE WHEN value_usd > 0 THEN 1 END) as positive_in_unified
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_unified/**/*.parquet')
    WHERE schema_source = 'old_schema'
""").fetchone()

total_unified, null_unified, zero_unified, pos_unified = unified_null_check
print(f"  Total old records in unified: {total_unified:,}")
print(f"  NULL in unified: {null_unified:,}")
print(f"  Zero in unified: {zero_unified:,}")
print(f"  Positive in unified: {pos_unified:,}")

print(f"\n🔍 ISSUE IDENTIFIED:")
print(f"Our migration used COALESCE(value_usd, 0) which converted NULL to 0")
print(f"This means we lost {null_usd:,} records that could potentially have USD values calculated")

conn.close()
