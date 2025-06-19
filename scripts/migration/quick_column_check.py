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

print("OLD SCHEMA COLUMNS:")
old_cols = conn.execute("""
    DESCRIBE SELECT * FROM read_parquet('s3://solana-data/bronze/wallet_transactions_old_schema/**/*.parquet') LIMIT 1
""").fetchall()

for col in old_cols:
    print(f"  {col[0]}")

print("\nCURRENT SCHEMA COLUMNS:")
current_cols = conn.execute("""
    DESCRIBE SELECT * FROM read_parquet('s3://solana-data/bronze/wallet_transactions/**/wallet_transactions_*.parquet', union_by_name=true) LIMIT 1
""").fetchall()

for col in current_cols:
    print(f"  {col[0]}")

conn.close()
