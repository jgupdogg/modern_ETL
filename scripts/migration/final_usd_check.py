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

print("FINAL CHECK: Old schema USD values vs our migration calculation...")

# Check what we actually have vs what we calculated
comparison = conn.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN value_usd > 0 THEN 1 END) as original_usd_records,
        COUNT(CASE WHEN value_usd = 0 THEN 1 END) as zero_usd_records,
        -- Check if zero USD records have price data for calculation
        COUNT(CASE WHEN value_usd = 0 AND base_price > 0 THEN 1 END) as zero_usd_with_base_price,
        COUNT(CASE WHEN value_usd = 0 AND quote_price > 0 THEN 1 END) as zero_usd_with_quote_price,
        -- Sample calculations
        AVG(CASE WHEN value_usd = 0 AND base_price > 0 THEN from_amount * base_price END) as avg_calc_base,
        AVG(CASE WHEN value_usd = 0 AND quote_price > 0 THEN to_amount * quote_price END) as avg_calc_quote
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
""").fetchone()

total, orig_usd, zero_usd, zero_w_base, zero_w_quote, avg_base_calc, avg_quote_calc = comparison

print(f"OLD SCHEMA ANALYSIS:")
print(f"  Total records: {total:,}")
print(f"  Records with original USD values: {orig_usd:,} ({orig_usd/total*100:.1f}%)")
print(f"  Records with zero USD: {zero_usd:,} ({zero_usd/total*100:.1f}%)")
print(f"  Zero USD records with base_price: {zero_w_base:,}")
print(f"  Zero USD records with quote_price: {zero_w_quote:,}")

if avg_base_calc:
    print(f"  Avg calculable value (base): ${avg_base_calc:.2f}")
if avg_quote_calc:
    print(f"  Avg calculable value (quote): ${avg_quote_calc:.2f}")

# Check what happened in our unified migration
unified_check = conn.execute("""
    SELECT 
        COUNT(*) as total_old_records,
        COUNT(CASE WHEN value_usd > 0 THEN 1 END) as unified_usd_records,
        AVG(CASE WHEN value_usd > 0 THEN value_usd END) as avg_usd_value
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_unified/**/*.parquet')
    WHERE schema_source = 'old_schema'
""").fetchone()

print(f"\nUNIFIED MIGRATION RESULTS FOR OLD SCHEMA:")
print(f"  Old schema records in unified: {unified_check[0]:,}")
print(f"  Records with USD values: {unified_check[1]:,}")
print(f"  Avg USD value: ${unified_check[2]:.2f}" if unified_check[2] else "  Avg USD: N/A")

# Sample zero USD records from old schema to see what we're missing
zero_sample = conn.execute("""
    SELECT 
        from_symbol, to_symbol, from_amount, to_amount, 
        base_price, quote_price, value_usd,
        -- What we could calculate
        CASE WHEN base_price > 0 THEN from_amount * base_price ELSE 0 END as calc_from_base,
        CASE WHEN quote_price > 0 THEN to_amount * quote_price ELSE 0 END as calc_from_quote
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
    WHERE value_usd = 0
    LIMIT 5
""").fetchall()

print(f"\nSAMPLE ZERO USD RECORDS WITH CALCULATION POTENTIAL:")
for row in zero_sample:
    from_sym, to_sym, from_amt, to_amt, base_p, quote_p, orig_usd, calc_base, calc_quote = row
    print(f"  {from_sym} -> {to_sym}")
    print(f"    Original USD: ${orig_usd:.2f}")
    print(f"    Could calculate: base method=${calc_base:.2f}, quote method=${calc_quote:.2f}")

print(f"\n💡 CONCLUSION:")
print(f"The old schema has excellent price and amount data\!")
print(f"We could potentially calculate USD values for {zero_usd:,} additional records")
print(f"But those records currently show value_usd = 0, meaning the original calculation failed/skipped them")

conn.close()
