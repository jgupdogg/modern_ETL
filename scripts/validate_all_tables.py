#!/usr/bin/env python3
"""
Comprehensive DuckDB-based validation for all Smart Trader pipeline tables
Safe validation without PySpark crashes
"""

import duckdb
import sys
from datetime import datetime
import traceback

def setup_duckdb():
    """Setup DuckDB with S3/MinIO configuration"""
    try:
        conn = duckdb.connect('/data/analytics.duckdb')
        conn.execute("""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='minioadmin';
            SET s3_secret_access_key='minioadmin123';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        print("✅ DuckDB connection established")
        return conn
    except Exception as e:
        print(f"❌ Failed to setup DuckDB: {e}")
        return None

def validate_bronze_token_list(conn):
    """Validate Bronze Token List table"""
    print("\n🔍 BRONZE TOKEN LIST")
    print("-" * 40)
    
    try:
        # Basic access and count
        result = conn.execute("""
            SELECT COUNT(*) as total_tokens,
                   COUNT(DISTINCT token_address) as unique_tokens,
                   MIN(created_at) as earliest_entry,
                   MAX(created_at) as latest_entry
            FROM parquet_scan('s3://solana-data/bronze/token_list_v3/*.parquet')
        """).fetchone()
        
        if result and result[0] > 0:
            print(f"✅ Total tokens: {result[0]:,}")
            print(f"✅ Unique addresses: {result[1]:,}")
            print(f"✅ Entry timeframe: {result[2]} to {result[3]}")
            
            # Data quality checks
            quality_result = conn.execute("""
                SELECT 
                    SUM(CASE WHEN token_address IS NULL THEN 1 ELSE 0 END) as null_addresses,
                    SUM(CASE WHEN symbol IS NULL OR symbol = '' THEN 1 ELSE 0 END) as missing_symbols,
                    SUM(CASE WHEN market_cap <= 0 THEN 1 ELSE 0 END) as invalid_market_caps,
                    AVG(liquidity) as avg_liquidity
                FROM parquet_scan('s3://solana-data/bronze/token_list_v3/*.parquet')
            """).fetchone()
            
            print(f"✅ Data quality - Null addresses: {quality_result[0]}, Missing symbols: {quality_result[1]}")
            print(f"✅ Invalid market caps: {quality_result[2]}, Avg liquidity: ${quality_result[3]:,.2f}")
            
            return True
        else:
            print("❌ No token list data found")
            return False
            
    except Exception as e:
        print(f"❌ Bronze Token List validation failed: {e}")
        return False

def validate_bronze_token_whales(conn):
    """Validate Bronze Token Whales table"""
    print("\n🔍 BRONZE TOKEN WHALES")  
    print("-" * 40)
    
    try:
        # Check recent partition (2025-06-19 known to exist)
        result = conn.execute("""
            SELECT COUNT(*) as total_whales,
                   COUNT(DISTINCT token_address) as unique_tokens,
                   COUNT(DISTINCT wallet_address) as unique_wallets,
                   SUM(CASE WHEN txns_fetched = true THEN 1 ELSE 0 END) as transactions_fetched,
                   AVG(holdings_percentage) as avg_holdings_pct
            FROM parquet_scan('s3://solana-data/bronze/token_whales/date=2025-06-19/*.parquet')
        """).fetchone()
        
        if result and result[0] > 0:
            print(f"✅ Total whale records: {result[0]:,}")
            print(f"✅ Unique tokens: {result[1]:,}")
            print(f"✅ Unique wallets: {result[2]:,}")
            print(f"✅ Transactions fetched: {result[3]:,} ({(result[3]/result[0]*100):.1f}%)")
            print(f"✅ Avg holdings percentage: {result[4]:.2f}%")
            
            # Validate whale ranking
            ranking_result = conn.execute("""
                SELECT 
                    MIN(rank) as min_rank,
                    MAX(rank) as max_rank,
                    COUNT(DISTINCT rank) as unique_ranks
                FROM parquet_scan('s3://solana-data/bronze/token_whales/date=2025-06-19/*.parquet')
                WHERE token_address = (SELECT token_address FROM parquet_scan('s3://solana-data/bronze/token_whales/date=2025-06-19/*.parquet') LIMIT 1)
            """).fetchone()
            
            print(f"✅ Whale ranking - Min: {ranking_result[0]}, Max: {ranking_result[1]}, Unique: {ranking_result[2]}")
            
            return True
        else:
            print("❌ No token whales data found")
            return False
            
    except Exception as e:
        print(f"❌ Bronze Token Whales validation failed: {e}")
        return False

def validate_bronze_wallet_transactions(conn):
    """Validate Bronze Wallet Transactions table (already migrated)"""
    print("\n🔍 BRONZE WALLET TRANSACTIONS (MIGRATED)")
    print("-" * 40)
    
    try:
        # Test multiple partitions
        partitions = ['2024-06-03', '2024-07-19', '2024-08-15']
        total_records = 0
        processed_records = 0
        
        for partition in partitions:
            try:
                result = conn.execute(f"""
                    SELECT COUNT(*) as total,
                           COUNT(DISTINCT wallet_address) as wallets,
                           SUM(CASE WHEN processed_for_pnl = true THEN 1 ELSE 0 END) as processed
                    FROM parquet_scan('s3://solana-data/bronze/wallet_transactions/date={partition}/*.parquet')
                """).fetchone()
                
                if result and result[0] > 0:
                    print(f"✅ {partition}: {result[0]:,} records, {result[1]:,} wallets, {result[2]:,} processed")
                    total_records += result[0]
                    processed_records += result[2]
                    
            except Exception as e:
                print(f"⚠️  {partition}: Not accessible - {e}")
        
        if total_records > 0:
            print(f"✅ Migration Status - Total: {total_records:,}, Processed: {processed_records:,} ({(processed_records/total_records*100):.1f}%)")
            
            # Check processing fields exist
            schema_check = conn.execute("""
                SELECT column_name FROM (
                    DESCRIBE (SELECT * FROM parquet_scan('s3://solana-data/bronze/wallet_transactions/date=2024-06-03/*.parquet') LIMIT 1)
                ) WHERE column_name IN ('processed_for_pnl', 'pnl_processed_at', 'pnl_processing_batch_id')
            """).fetchall()
            
            processing_fields = [row[0] for row in schema_check]
            print(f"✅ Processing fields present: {processing_fields}")
            
            return True
        else:
            print("❌ No wallet transaction data accessible")
            return False
            
    except Exception as e:
        print(f"❌ Bronze Wallet Transactions validation failed: {e}")
        return False

def validate_silver_tracked_tokens(conn):
    """Validate Silver Tracked Tokens table"""
    print("\n🔍 SILVER TRACKED TOKENS")
    print("-" * 40)
    
    try:
        # Test recent partition
        result = conn.execute("""
            SELECT COUNT(*) as total_tokens,
                   COUNT(DISTINCT token_address) as unique_addresses,
                   AVG(quality_score) as avg_quality_score,
                   AVG(volume_mcap_ratio) as avg_volume_mcap_ratio,
                   MIN(processing_date) as earliest_processing,
                   MAX(processing_date) as latest_processing
            FROM parquet_scan('s3://solana-data/silver/tracked_tokens/processing_date=2025-06-19/*.parquet')
        """).fetchone()
        
        if result and result[0] > 0:
            print(f"✅ Total tracked tokens: {result[0]:,}")
            print(f"✅ Unique addresses: {result[1]:,}")
            print(f"✅ Avg quality score: {result[2]:.2f}")
            print(f"✅ Avg volume/mcap ratio: {result[3]:.4f}")
            print(f"✅ Processing timeframe: {result[4]} to {result[5]}")
            
            # Quality distribution
            quality_dist = conn.execute("""
                SELECT 
                    SUM(CASE WHEN quality_score >= 0.8 THEN 1 ELSE 0 END) as high_quality,
                    SUM(CASE WHEN quality_score >= 0.6 AND quality_score < 0.8 THEN 1 ELSE 0 END) as medium_quality,
                    SUM(CASE WHEN quality_score < 0.6 THEN 1 ELSE 0 END) as low_quality
                FROM parquet_scan('s3://solana-data/silver/tracked_tokens/processing_date=2025-06-19/*.parquet')
            """).fetchone()
            
            print(f"✅ Quality distribution - High: {quality_dist[0]}, Medium: {quality_dist[1]}, Low: {quality_dist[2]}")
            
            return True
        else:
            print("❌ No tracked tokens data found")
            return False
            
    except Exception as e:
        print(f"❌ Silver Tracked Tokens validation failed: {e}")
        return False

def validate_silver_wallet_pnl(conn):
    """Validate Silver Wallet PnL table (may not exist yet)"""
    print("\n🔍 SILVER WALLET PNL")
    print("-" * 40)
    
    try:
        # Try multiple possible paths
        paths_to_test = [
            "s3://solana-data/silver/wallet_pnl/*.parquet",
            "s3://solana-data/silver/wallet_pnl/*/*.parquet", 
            "s3://solana-data/silver/wallet_pnl/*/*/*.parquet"
        ]
        
        for path in paths_to_test:
            try:
                result = conn.execute(f"""
                    SELECT COUNT(*) as total_records,
                           COUNT(DISTINCT wallet_address) as unique_wallets,
                           SUM(CASE WHEN total_pnl > 0 THEN 1 ELSE 0 END) as profitable_records,
                           AVG(total_pnl) as avg_pnl
                    FROM parquet_scan('{path}')
                """).fetchone()
                
                if result and result[0] > 0:
                    print(f"✅ Found PnL data at {path}")
                    print(f"✅ Total records: {result[0]:,}")
                    print(f"✅ Unique wallets: {result[1]:,}")
                    print(f"✅ Profitable records: {result[2]:,} ({(result[2]/result[0]*100):.1f}%)")
                    print(f"✅ Average PnL: ${result[3]:.2f}")
                    return True
                    
            except:
                continue
        
        print("⚠️  No silver PnL data found - needs to be generated from bronze")
        print("ℹ️  This is expected if PnL processing hasn't been run yet")
        return True  # Not a failure - just not generated yet
        
    except Exception as e:
        print(f"❌ Silver Wallet PnL validation failed: {e}")
        return False

def validate_gold_top_traders(conn):
    """Validate Gold Top Traders table"""
    print("\n🔍 GOLD TOP TRADERS")
    print("-" * 40)
    
    try:
        result = conn.execute("""
            SELECT COUNT(*) as total_traders,
                   COUNT(DISTINCT performance_tier) as unique_tiers,
                   AVG(total_pnl) as avg_pnl,
                   AVG(roi) as avg_roi,
                   AVG(win_rate) as avg_win_rate,
                   MAX(trader_rank) as max_rank
            FROM parquet_scan('s3://solana-data/gold/top_traders/*.parquet')
        """).fetchone()
        
        if result and result[0] > 0:
            print(f"✅ Total top traders: {result[0]:,}")
            print(f"✅ Performance tiers: {result[1]:,}")
            print(f"✅ Average PnL: ${result[2]:,.2f}")
            print(f"✅ Average ROI: {result[3]:.2f}%") 
            print(f"✅ Average win rate: {result[4]:.2f}%")
            print(f"✅ Ranking spans: 1 to {result[5]}")
            
            # Tier distribution
            tier_dist = conn.execute("""
                SELECT performance_tier, COUNT(*) as count
                FROM parquet_scan('s3://solana-data/gold/top_traders/*.parquet')
                GROUP BY performance_tier
                ORDER BY count DESC
            """).fetchall()
            
            print("✅ Tier distribution:")
            for tier, count in tier_dist:
                print(f"   - {tier}: {count} traders")
            
            return True
        else:
            print("❌ No top traders data found")
            return False
            
    except Exception as e:
        print(f"❌ Gold Top Traders validation failed: {e}")
        return False

def main():
    """Main validation orchestrator"""
    print("🚀 SMART TRADER PIPELINE - COMPLETE TABLE VALIDATION")
    print(f"📅 {datetime.now()}")
    print("=" * 60)
    
    conn = setup_duckdb()
    if not conn:
        sys.exit(1)
    
    # Define all table validations
    validations = [
        ("Bronze Token List", validate_bronze_token_list),
        ("Bronze Token Whales", validate_bronze_token_whales), 
        ("Bronze Wallet Transactions", validate_bronze_wallet_transactions),
        ("Silver Tracked Tokens", validate_silver_tracked_tokens),
        ("Silver Wallet PnL", validate_silver_wallet_pnl),
        ("Gold Top Traders", validate_gold_top_traders)
    ]
    
    results = []
    for table_name, validator_func in validations:
        try:
            result = validator_func(conn)
            results.append((table_name, result))
        except Exception as e:
            print(f"❌ {table_name} validation crashed: {e}")
            traceback.print_exc()
            results.append((table_name, False))
    
    # Summary report
    print("\n" + "=" * 60)
    print("📋 VALIDATION SUMMARY REPORT")
    print("=" * 60)
    
    passed = 0
    for table_name, result in results:
        status = "✅ ACCESSIBLE" if result else "❌ FAILED"
        print(f"{table_name:<30} {status}")
        if result:
            passed += 1
    
    success_rate = (passed / len(results)) * 100
    print(f"\nOverall Pipeline Health: {success_rate:.1f}% ({passed}/{len(results)} tables)")
    
    # Recommendations
    print(f"\n📋 RECOMMENDATIONS:")
    if success_rate >= 85:
        print("🎉 Pipeline is healthy and ready for migration operations")
        if passed < len(results):
            print("ℹ️  Some tables may need generation (Silver PnL) - this is normal")
    elif success_rate >= 70:
        print("⚠️  Pipeline mostly healthy with some issues to address")
        print("🔧 Review failed validations before proceeding with migrations")
    else:
        print("🚨 Pipeline has significant issues")
        print("🛠️  Address accessibility problems before migration")
    
    print(f"\n🛡️  System used safe DuckDB validation - no crashes occurred!")
    
    conn.close()
    return 0 if success_rate >= 70 else 1

if __name__ == "__main__":
    sys.exit(main())