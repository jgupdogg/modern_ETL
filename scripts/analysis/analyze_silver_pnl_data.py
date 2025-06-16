#!/usr/bin/env python3
"""
Analyze existing silver PnL data to understand performance distribution
and evaluate if gold layer criteria are appropriate
"""

import subprocess
import json
import sys

def analyze_silver_pnl_data():
    """Analyze silver PnL data via DuckDB container"""
    print("🔍 Analyzing Silver Wallet PnL Data...")
    
    # Create analysis script for DuckDB container
    analysis_script = '''
import duckdb
import json
import sys

# Connect to DuckDB
conn = duckdb.connect("/data/analytics.duckdb")

try:
    # Configure S3/MinIO access
    conn.execute("LOAD httpfs;")
    conn.execute("SET s3_endpoint='minio:9000';")
    conn.execute("SET s3_access_key_id='minioadmin';")
    conn.execute("SET s3_secret_access_key='minioadmin123';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    # Check if silver PnL data exists
    try:
        total_records = conn.execute("""
            SELECT COUNT(*) FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
        """).fetchone()[0]
        
        if total_records == 0:
            print("NO_SILVER_DATA")
            sys.exit(0)
            
        print(f"TOTAL_RECORDS: {total_records}")
        
    except Exception as e:
        print(f"ERROR_READING_DATA: {e}")
        sys.exit(1)
    
    # Analyze data distribution
    distribution_query = """
    WITH silver_data AS (
        SELECT 
            wallet_address,
            token_address,
            time_period,
            total_pnl,
            roi,
            win_rate,
            trade_count,
            processed_for_gold,
            COALESCE(processed_for_gold, false) as processed_gold_flag
        FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
    )
    SELECT 
        'overall_stats' as category,
        COUNT(*) as total_records,
        COUNT(DISTINCT wallet_address) as unique_wallets,
        SUM(CASE WHEN token_address = 'ALL_TOKENS' THEN 1 ELSE 0 END) as portfolio_records,
        SUM(CASE WHEN time_period = 'all' THEN 1 ELSE 0 END) as all_time_records,
        SUM(CASE WHEN processed_gold_flag = true THEN 1 ELSE 0 END) as already_processed,
        SUM(CASE WHEN processed_gold_flag = false OR processed_gold_flag IS NULL THEN 1 ELSE 0 END) as unprocessed
    FROM silver_data
    
    UNION ALL
    
    SELECT 
        'portfolio_performance' as category,
        COUNT(*) as records,
        AVG(total_pnl) as avg_pnl,
        MIN(total_pnl) as min_pnl,
        MAX(total_pnl) as max_pnl,
        AVG(roi) as avg_roi,
        AVG(win_rate) as avg_win_rate
    FROM silver_data 
    WHERE token_address = 'ALL_TOKENS' AND time_period = 'all'
    
    UNION ALL
    
    SELECT 
        'positive_pnl_stats' as category,
        COUNT(*) as positive_pnl_count,
        AVG(total_pnl) as avg_positive_pnl,
        MIN(total_pnl) as min_positive_pnl,
        MAX(total_pnl) as max_positive_pnl,
        AVG(roi) as avg_positive_roi,
        AVG(win_rate) as avg_positive_win_rate
    FROM silver_data 
    WHERE token_address = 'ALL_TOKENS' AND time_period = 'all' AND total_pnl > 0
    """
    
    results = conn.execute(distribution_query).fetchall()
    columns = [desc[0] for desc in conn.description]
    
    print("ANALYSIS_START")
    for row in results:
        row_dict = dict(zip(columns, row))
        print(json.dumps(row_dict))
    print("ANALYSIS_END")
    
    # Check how many would qualify for current gold criteria
    criteria_query = """
    WITH qualifying_wallets AS (
        SELECT 
            wallet_address,
            total_pnl,
            roi,
            win_rate, 
            trade_count,
            processed_for_gold,
            CASE 
                WHEN total_pnl >= 1000 AND roi >= 20 AND win_rate >= 60 AND trade_count >= 10 THEN 'current_criteria'
                WHEN total_pnl >= 500 AND roi >= 15 AND win_rate >= 55 AND trade_count >= 5 THEN 'relaxed_criteria'
                WHEN total_pnl > 0 THEN 'positive_only'
                ELSE 'negative_pnl'
            END as qualification_tier
        FROM read_parquet('s3://solana-data/silver/wallet_pnl/**/*.parquet')
        WHERE token_address = 'ALL_TOKENS' 
          AND time_period = 'all'
          AND (processed_for_gold = false OR processed_for_gold IS NULL)
    )
    SELECT 
        qualification_tier,
        COUNT(*) as wallet_count,
        AVG(total_pnl) as avg_pnl,
        AVG(roi) as avg_roi,
        AVG(win_rate) as avg_win_rate,
        AVG(trade_count) as avg_trades
    FROM qualifying_wallets
    GROUP BY qualification_tier
    ORDER BY 
        CASE qualification_tier 
            WHEN 'current_criteria' THEN 1
            WHEN 'relaxed_criteria' THEN 2  
            WHEN 'positive_only' THEN 3
            ELSE 4
        END
    """
    
    criteria_results = conn.execute(criteria_query).fetchall()
    criteria_columns = [desc[0] for desc in conn.description]
    
    print("CRITERIA_ANALYSIS_START") 
    for row in criteria_results:
        row_dict = dict(zip(criteria_columns, row))
        print(json.dumps(row_dict))
    print("CRITERIA_ANALYSIS_END")
    
    conn.close()

except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
'''
    
    # Execute analysis in DuckDB container
    cmd = ['docker', 'exec', 'claude_pipeline-duckdb', 'python3', '-c', analysis_script]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    
    if result.returncode != 0:
        print(f"❌ Analysis failed: {result.stderr}")
        return False
    
    # Parse results
    output_lines = result.stdout.split('\n')
    
    if "NO_SILVER_DATA" in output_lines:
        print("❌ No silver PnL data found")
        return False
    
    # Parse analysis results
    in_analysis = False
    in_criteria = False
    
    overall_stats = {}
    criteria_stats = []
    
    for line in output_lines:
        if "TOTAL_RECORDS:" in line:
            total = line.split(":")[1].strip()
            print(f"📊 Total Silver PnL Records: {total}")
            continue
            
        if line == "ANALYSIS_START":
            in_analysis = True
            continue
        elif line == "ANALYSIS_END":
            in_analysis = False
            continue
        elif line == "CRITERIA_ANALYSIS_START":
            in_criteria = True
            continue
        elif line == "CRITERIA_ANALYSIS_END":
            in_criteria = False
            continue
            
        if in_analysis and line.strip():
            try:
                data = json.loads(line)
                category = data.get('category', 'unknown')
                print(f"\n📈 {category.upper().replace('_', ' ')}:")
                for key, value in data.items():
                    if key != 'category' and value is not None:
                        if isinstance(value, float):
                            print(f"  {key}: {value:,.2f}")
                        else:
                            print(f"  {key}: {value:,}")
            except json.JSONDecodeError:
                continue
                
        if in_criteria and line.strip():
            try:
                data = json.loads(line)
                criteria_stats.append(data)
            except json.JSONDecodeError:
                continue
    
    # Display criteria analysis
    print(f"\n🎯 GOLD LAYER CRITERIA ANALYSIS:")
    print("=" * 60)
    
    for stats in criteria_stats:
        tier = stats.get('qualification_tier', 'unknown')
        count = stats.get('wallet_count', 0)
        avg_pnl = stats.get('avg_pnl', 0)
        avg_roi = stats.get('avg_roi', 0)
        avg_win_rate = stats.get('avg_win_rate', 0)
        avg_trades = stats.get('avg_trades', 0)
        
        print(f"\n🔹 {tier.upper().replace('_', ' ')}:")
        print(f"  Qualifying Wallets: {count:,}")
        if count > 0:
            print(f"  Avg PnL: ${avg_pnl:,.2f}")
            print(f"  Avg ROI: {avg_roi:.1f}%")
            print(f"  Avg Win Rate: {avg_win_rate:.1f}%")
            print(f"  Avg Trades: {avg_trades:.1f}")
    
    # Make recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    
    current_criteria_count = 0
    relaxed_criteria_count = 0
    positive_only_count = 0
    
    for stats in criteria_stats:
        tier = stats.get('qualification_tier')
        count = stats.get('wallet_count', 0)
        
        if tier == 'current_criteria':
            current_criteria_count = count
        elif tier == 'relaxed_criteria':
            relaxed_criteria_count = count
        elif tier == 'positive_only':
            positive_only_count = count
    
    if current_criteria_count == 0:
        print("❌ Current criteria too stringent - no qualifying wallets")
        if relaxed_criteria_count > 0:
            print(f"✅ Consider relaxed criteria: $500+ PnL, 15%+ ROI, 55%+ win rate, 5+ trades")
            print(f"   Would qualify {relaxed_criteria_count} wallets")
        elif positive_only_count > 0:
            print(f"✅ Consider positive PnL only as starting criteria")
            print(f"   Would qualify {positive_only_count} wallets")
        else:
            print("⚠️ No positive PnL wallets found - check silver data quality")
    else:
        print(f"✅ Current criteria appropriate: {current_criteria_count} qualifying wallets")
    
    return True

def main():
    """Run silver PnL data analysis"""
    print("🚀 Silver PnL Data Analysis")
    print("=" * 50)
    
    success = analyze_silver_pnl_data()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ Analysis completed successfully")
    else:
        print("❌ Analysis failed")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)