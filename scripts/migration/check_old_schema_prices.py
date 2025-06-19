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

print("Analyzing OLD SCHEMA price and amount fields for USD value calculation...")

# Get complete old schema structure
print("\n1. OLD SCHEMA COMPLETE FIELD LIST:")
old_schema_fields = conn.execute("""
    DESCRIBE SELECT * FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet') LIMIT 1
""").fetchall()

for i, (field_name, field_type) in enumerate(old_schema_fields):
    print(f"  {i+1:2d}. {field_name:<25}  < /dev/null |  {field_type}")

# Focus on price and amount related fields
print("\n2. PRICE AND AMOUNT FIELD AVAILABILITY:")
price_amount_analysis = conn.execute("""
    SELECT 
        COUNT(*) as total_records,
        -- Amount fields
        COUNT(CASE WHEN from_amount > 0 THEN 1 END) as from_amount_available,
        COUNT(CASE WHEN to_amount > 0 THEN 1 END) as to_amount_available,
        -- Price fields  
        COUNT(CASE WHEN base_price > 0 THEN 1 END) as base_price_available,
        COUNT(CASE WHEN quote_price > 0 THEN 1 END) as quote_price_available,
        COUNT(CASE WHEN nearest_price > 0 THEN 1 END) as nearest_price_available,
        -- USD value field
        COUNT(CASE WHEN value_usd > 0 THEN 1 END) as value_usd_available,
        -- Stats
        AVG(CASE WHEN from_amount > 0 THEN from_amount END) as avg_from_amount,
        AVG(CASE WHEN to_amount > 0 THEN to_amount END) as avg_to_amount,
        AVG(CASE WHEN base_price > 0 THEN base_price END) as avg_base_price,
        AVG(CASE WHEN quote_price > 0 THEN quote_price END) as avg_quote_price,
        AVG(CASE WHEN nearest_price > 0 THEN nearest_price END) as avg_nearest_price
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
""").fetchone()

total = price_amount_analysis[0]
print(f"  Total Records: {total:,}")
print(f"\n  AMOUNT FIELDS:")
print(f"    from_amount available: {price_amount_analysis[1]:,} ({price_amount_analysis[1]/total*100:.1f}%)")
print(f"    to_amount available: {price_amount_analysis[2]:,} ({price_amount_analysis[2]/total*100:.1f}%)")
print(f"    Avg from_amount: {price_amount_analysis[7]:.4f}" if price_amount_analysis[7] else "    Avg from_amount: N/A")
print(f"    Avg to_amount: {price_amount_analysis[8]:.4f}" if price_amount_analysis[8] else "    Avg to_amount: N/A")

print(f"\n  PRICE FIELDS:")
print(f"    base_price available: {price_amount_analysis[3]:,} ({price_amount_analysis[3]/total*100:.1f}%)")
print(f"    quote_price available: {price_amount_analysis[4]:,} ({price_amount_analysis[4]/total*100:.1f}%)")
print(f"    nearest_price available: {price_amount_analysis[5]:,} ({price_amount_analysis[5]/total*100:.1f}%)")
print(f"    value_usd available: {price_amount_analysis[6]:,} ({price_amount_analysis[6]/total*100:.1f}%)")

print(f"\n  PRICE AVERAGES:")
print(f"    Avg base_price: ${price_amount_analysis[9]:.6f}" if price_amount_analysis[9] else "    Avg base_price: N/A")
print(f"    Avg quote_price: ${price_amount_analysis[10]:.6f}" if price_amount_analysis[10] else "    Avg quote_price: N/A") 
print(f"    Avg nearest_price: ${price_amount_analysis[11]:.6f}" if price_amount_analysis[11] else "    Avg nearest_price: N/A")

# Sample records to see actual data patterns
print("\n3. SAMPLE OLD SCHEMA RECORDS (with and without USD values):")

# Records WITH USD values
with_usd_sample = conn.execute("""
    SELECT 
        SUBSTR(wallet_address, 1, 8) || '...' as wallet,
        from_symbol || ' -> ' || to_symbol as trade,
        from_amount,
        to_amount,
        base_price,
        quote_price,
        nearest_price,
        value_usd,
        transaction_type
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
    WHERE value_usd > 0
    ORDER BY value_usd DESC
    LIMIT 3
""").fetchall()

print(f"\n  RECORDS WITH USD VALUES:")
for row in with_usd_sample:
    wallet, trade, from_amt, to_amt, base_p, quote_p, nearest_p, usd_val, tx_type = row
    print(f"    {wallet} | {trade} | from:{from_amt:.4f} to:{to_amt:.4f}")
    print(f"      Prices: base:${base_p:.6f} quote:${quote_p:.6f} nearest:${nearest_p:.6f} | USD:${usd_val:.2f} | {tx_type}")

# Records WITHOUT USD values but with price data
without_usd_sample = conn.execute("""
    SELECT 
        SUBSTR(wallet_address, 1, 8) || '...' as wallet,
        from_symbol || ' -> ' || to_symbol as trade,
        from_amount,
        to_amount,
        base_price,
        quote_price,
        nearest_price,
        value_usd,
        transaction_type
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
    WHERE value_usd = 0 AND (base_price > 0 OR quote_price > 0 OR nearest_price > 0)
    ORDER BY from_amount DESC
    LIMIT 3
""").fetchall()

print(f"\n  RECORDS WITHOUT USD BUT WITH PRICE DATA:")
for row in without_usd_sample:
    wallet, trade, from_amt, to_amt, base_p, quote_p, nearest_p, usd_val, tx_type = row
    print(f"    {wallet} | {trade} | from:{from_amt:.4f} to:{to_amt:.4f}")
    print(f"      Prices: base:${base_p:.6f} quote:${quote_p:.6f} nearest:${nearest_p:.6f} | USD:${usd_val:.2f} | {tx_type}")

# Check if we can calculate missing USD values
print("\n4. POTENTIAL USD VALUE CALCULATION OPPORTUNITIES:")
calc_opportunity = conn.execute("""
    SELECT 
        COUNT(*) as total_no_usd,
        COUNT(CASE WHEN base_price > 0 AND from_amount > 0 THEN 1 END) as can_calc_from_base,
        COUNT(CASE WHEN quote_price > 0 AND to_amount > 0 THEN 1 END) as can_calc_from_quote,
        COUNT(CASE WHEN nearest_price > 0 AND from_amount > 0 THEN 1 END) as can_calc_from_nearest,
        COUNT(CASE WHEN (base_price > 0 AND from_amount > 0) OR 
                         (quote_price > 0 AND to_amount > 0) OR 
                         (nearest_price > 0 AND from_amount > 0) THEN 1 END) as total_calculable
    FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet')
    WHERE value_usd = 0
""").fetchone()

total_no_usd, calc_base, calc_quote, calc_nearest, total_calc = calc_opportunity
print(f"  Records without USD values: {total_no_usd:,}")
print(f"  Can calculate from base_price * from_amount: {calc_base:,}")
print(f"  Can calculate from quote_price * to_amount: {calc_quote:,}")
print(f"  Can calculate from nearest_price * from_amount: {calc_nearest:,}")
print(f"  Total calculable: {total_calc:,} ({total_calc/total_no_usd*100:.1f}% of missing)")

print(f"\n5. SUMMARY FOR PnL CALCULATIONS:")
print(f"  ✅ Old schema HAS amount data: from_amount ({price_amount_analysis[1]/total*100:.1f}%), to_amount ({price_amount_analysis[2]/total*100:.1f}%)")
print(f"  ✅ Old schema HAS price data: base_price ({price_amount_analysis[3]/total*100:.1f}%), quote_price ({price_amount_analysis[4]/total*100:.1f}%), nearest_price ({price_amount_analysis[5]/total*100:.1f}%)")
print(f"  💡 Can improve USD calculation for {total_calc:,} additional records ({total_calc/total*100:.1f}% of total)")

conn.close()
