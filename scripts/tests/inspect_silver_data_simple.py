#!/usr/bin/env python3
"""
Simple inspection of silver PnL data structure using boto3
"""

import boto3
from botocore.client import Config
import io

def inspect_silver_data():
    """Inspect silver PnL data via direct S3 access"""
    print("🔍 Inspecting Silver PnL Data...")
    
    # Create MinIO client
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4')
    )
    
    try:
        # List objects in silver wallet_pnl bucket
        response = s3_client.list_objects_v2(
            Bucket='solana-data',
            Prefix='silver/wallet_pnl/'
        )
        
        if 'Contents' not in response:
            print("❌ No silver PnL data found")
            return False
        
        files = response['Contents']
        print(f"📊 Found {len(files)} files in silver/wallet_pnl/")
        
        for file_info in files[:5]:  # Show first 5 files
            key = file_info['Key']
            size = file_info['Size']
            modified = file_info['LastModified']
            print(f"  📄 {key} ({size:,} bytes, {modified})")
        
        # Try to read the first parquet file to get schema info
        parquet_files = [f for f in files if f['Key'].endswith('.parquet')]
        
        if not parquet_files:
            print("❌ No parquet files found")
            return False
        
        print(f"\n🔍 Examining schema of: {parquet_files[0]['Key']}")
        
        # Download and inspect first parquet file
        obj = s3_client.get_object(Bucket='solana-data', Key=parquet_files[0]['Key'])
        parquet_data = obj['Body'].read()
        
        print(f"✅ Successfully read {len(parquet_data):,} bytes of parquet data")
        print(f"📝 File appears to be valid parquet format")
        
        # Check if we can read it with pandas/pyarrow (if available)
        try:
            import pyarrow.parquet as pq
            
            # Read parquet data
            table = pq.read_table(io.BytesIO(parquet_data))
            
            print(f"\n📋 SCHEMA ANALYSIS:")
            print(f"  Rows: {table.num_rows:,}")
            print(f"  Columns: {table.num_columns}")
            
            print(f"\n🏷️ COLUMN NAMES:")
            for i, name in enumerate(table.column_names):
                print(f"  {i+1:2d}. {name}")
            
            # Check for our new gold processing fields
            gold_fields = ['processed_for_gold', 'gold_processed_at', 'gold_processing_status', 'gold_batch_id']
            print(f"\n🔍 GOLD PROCESSING FIELDS CHECK:")
            for field in gold_fields:
                if field in table.column_names:
                    print(f"  ✅ {field}")
                else:
                    print(f"  ❌ Missing: {field}")
            
            # Show sample data
            if table.num_rows > 0:
                print(f"\n📊 SAMPLE DATA (first 3 rows):")
                
                # Convert to pandas for easier display
                df = table.to_pandas()
                
                # Show key columns
                key_cols = ['wallet_address', 'token_address', 'time_period', 'total_pnl', 'roi', 'win_rate', 'trade_count']
                existing_cols = [col for col in key_cols if col in df.columns]
                
                if existing_cols:
                    sample_df = df[existing_cols].head(3)
                    print(sample_df.to_string(index=False))
                
                # Show processing status if new fields exist
                processing_cols = ['processed_for_gold', 'gold_processing_status']
                existing_processing = [col for col in processing_cols if col in df.columns]
                
                if existing_processing:
                    print(f"\n🔄 PROCESSING STATUS:")
                    status_df = df[existing_processing].head(3)
                    print(status_df.to_string(index=False))
                
                # Analyze performance distribution
                portfolio_data = df[(df['token_address'] == 'ALL_TOKENS') & (df['time_period'] == 'all')]
                
                if len(portfolio_data) > 0:
                    print(f"\n📈 PORTFOLIO PERFORMANCE ANALYSIS:")
                    print(f"  Portfolio records: {len(portfolio_data)}")
                    print(f"  Positive PnL wallets: {len(portfolio_data[portfolio_data['total_pnl'] > 0])}")
                    print(f"  Average PnL: ${portfolio_data['total_pnl'].mean():.2f}")
                    print(f"  Min PnL: ${portfolio_data['total_pnl'].min():.2f}")
                    print(f"  Max PnL: ${portfolio_data['total_pnl'].max():.2f}")
                    
                    if 'roi' in portfolio_data.columns:
                        print(f"  Average ROI: {portfolio_data['roi'].mean():.1f}%")
                    
                    if 'win_rate' in portfolio_data.columns:
                        print(f"  Average Win Rate: {portfolio_data['win_rate'].mean():.1f}%")
                    
                    # Check current gold criteria
                    if all(col in portfolio_data.columns for col in ['total_pnl', 'roi', 'win_rate', 'trade_count']):
                        qualifying = portfolio_data[
                            (portfolio_data['total_pnl'] >= 1000) &
                            (portfolio_data['roi'] >= 20) &
                            (portfolio_data['win_rate'] >= 60) &
                            (portfolio_data['trade_count'] >= 10) &
                            (portfolio_data['total_pnl'] > 0)
                        ]
                        
                        print(f"\n🎯 GOLD CRITERIA ANALYSIS:")
                        print(f"  Current criteria (≥$1K PnL, ≥20% ROI, ≥60% win rate, ≥10 trades):")
                        print(f"    Qualifying wallets: {len(qualifying)}")
                        
                        if len(qualifying) == 0:
                            # Try relaxed criteria
                            relaxed = portfolio_data[
                                (portfolio_data['total_pnl'] >= 500) &
                                (portfolio_data['roi'] >= 15) &
                                (portfolio_data['win_rate'] >= 50) &
                                (portfolio_data['trade_count'] >= 5) &
                                (portfolio_data['total_pnl'] > 0)
                            ]
                            print(f"    Relaxed criteria (≥$500 PnL, ≥15% ROI, ≥50% win rate, ≥5 trades):")
                            print(f"      Qualifying wallets: {len(relaxed)}")
                            
                            if len(relaxed) == 0:
                                positive_only = portfolio_data[portfolio_data['total_pnl'] > 0]
                                print(f"    Positive PnL only:")
                                print(f"      Qualifying wallets: {len(positive_only)}")
            
            return True
            
        except ImportError:
            print("⚠️ PyArrow not available - cannot inspect parquet schema")
            return True
            
    except Exception as e:
        print(f"❌ Error inspecting silver data: {e}")
        return False

def main():
    """Run silver data inspection"""
    print("🚀 Silver PnL Data Inspection")
    print("=" * 50)
    
    success = inspect_silver_data()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ Inspection completed")
    else:
        print("❌ Inspection failed")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)