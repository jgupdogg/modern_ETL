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

print("Analyzing OLD SCHEMA price and amount fields...")

# Get old schema structure  
old_schema_fields = conn.execute("""
    DESCRIBE SELECT * FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet') LIMIT 1
""").fetchall()

print(f"\n1. OLD SCHEMA FIELDS ({len(old_schema_fields)} total):")
for i, field_info in enumerate(old_schema_fields):
    field_name = field_info[0]
    field_type = field_info[1] if len(field_info) > 1 else "unknown"
    marker = " 🔸" if any(keyword in field_name.lower() for keyword in ['price', 'amount', 'value', 'usd']) else ""
    print(f"  {i+1:2d}. {field_name:<25}  < /dev/null |  {field_type}{marker}")

# Check data availability
print(f"\n2. PRICE/AMOUNT DATA AVAILABILITY:")
availability = conn.execute("""
    SELECT 
        COUNT(*) as total,
        -- Amount fields
        COUNT(CASE WHEN from_amount > 0 THEN 1 END) as from_amount_avail,
        COUNT(CASE WHEN to_amount > 0 THEN 1 END) as to_amount_avail,
        -- Price fields available in old schema
        COUNT(CASE WHEN base_price > 0 THEN 1 END) as base_price_avail,
        COUNT(CASE WHEN quote_price > 0 THEN 1 END) as quote_price_avail,
        COUNT(CASE WHEN value_usd > 0 THEN 1 END) as value_usd_avail
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
""").fetchone()

total = availability[0]
print(f"  Total records: {total:,}")
print(f"  from_amount available: {availability[1]:,} ({availability[1]/total*100:.1f}%)")
print(f"  to_amount available: {availability[2]:,} ({availability[2]/total*100:.1f}%)")
print(f"  base_price available: {availability[3]:,} ({availability[3]/total*100:.1f}%)")
print(f"  quote_price available: {availability[4]:,} ({availability[4]/total*100:.1f}%)")
print(f"  value_usd available: {availability[5]:,} ({availability[5]/total*100:.1f}%)")

# Sample records
print(f"\n3. SAMPLE DATA:")
sample = conn.execute("""
    SELECT 
        from_symbol, to_symbol, from_amount, to_amount, 
        base_price, quote_price, value_usd, transaction_type
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
    WHERE from_amount > 0 AND to_amount > 0
    LIMIT 5
""").fetchall()

for row in sample:
    from_sym, to_sym, from_amt, to_amt, base_p, quote_p, usd_val, tx_type = row
    print(f"  {from_sym} -> {to_sym} | from:{from_amt:.4f} to:{to_amt:.4f} | base:${base_p:.6f} quote:${quote_p:.6f} | USD:${usd_val:.2f} | {tx_type}")

print(f"\n4. POTENTIAL FOR BETTER USD CALCULATION:")
calc_potential = conn.execute("""
    SELECT 
        COUNT(CASE WHEN value_usd = 0 THEN 1 END) as missing_usd,
        COUNT(CASE WHEN value_usd = 0 AND base_price > 0 AND from_amount > 0 THEN 1 END) as can_calc_base,
        COUNT(CASE WHEN value_usd = 0 AND quote_price > 0 AND to_amount > 0 THEN 1 END) as can_calc_quote
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
""").fetchone()

missing_usd, can_base, can_quote = calc_potential
print(f"  Records missing USD: {missing_usd:,}")
print(f"  Can calculate from base_price * from_amount: {can_base:,}")
print(f"  Can calculate from quote_price * to_amount: {can_quote:,}")
total_improvable = max(can_base, can_quote)
print(f"  Total improvable: {total_improvable:,} ({total_improvable/missing_usd*100:.1f}% of missing)")

conn.close()
