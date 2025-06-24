#!/usr/bin/env python3
"""
Simple Delta Lake Test - Create Delta table from sample data
Tests the core Delta Lake functionality without S3A dependencies first
"""

import sys
import os
import tempfile

# Add dags directory to path
sys.path.append('/opt/airflow/dags')

def test_local_delta_table():
    """Test creating a Delta table locally first"""
    print("🧪 Testing local Delta table creation...")
    
    try:
        from utils.delta_utils import create_delta_spark_session, DeltaLakeManager
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        from pyspark.sql import Row
        
        # Create Spark session
        spark = create_delta_spark_session("TestLocalDelta")
        delta_manager = DeltaLakeManager(spark)
        
        print(f"✅ Spark session created: {spark.sparkContext.appName}")
        
        # Create sample data that mimics our bronze token structure
        schema = StructType([
            StructField("token_address", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("liquidity", DoubleType(), True),
            StructField("volume_24h_usd", DoubleType(), True),
            StructField("price", DoubleType(), True)
        ])
        
        sample_data = [
            Row(token_address="So11111111111111111111111111111111111111112", 
                symbol="SOL", liquidity=1000000.0, volume_24h_usd=50000000.0, price=100.0),
            Row(token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 
                symbol="USDC", liquidity=2000000.0, volume_24h_usd=75000000.0, price=1.0),
            Row(token_address="Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", 
                symbol="USDT", liquidity=1500000.0, volume_24h_usd=60000000.0, price=1.0)
        ]
        
        df = spark.createDataFrame(sample_data, schema)
        print(f"✅ Sample DataFrame created with {df.count()} rows")
        
        # Create temporary local directory for Delta table
        with tempfile.TemporaryDirectory() as temp_dir:
            delta_table_path = f"{temp_dir}/test_delta_tokens"
            
            print(f"📁 Creating Delta table at: {delta_table_path}")
            
            # Test Delta table creation
            result = delta_manager.create_or_replace_table(
                df, 
                delta_table_path,
                partition_cols=None,  # No partitioning for simple test
                overwrite=True
            )
            
            print(f"✅ Delta table created successfully!")
            print(f"   Records: {result['records']}")
            print(f"   Version: {result['version']}")
            print(f"   Status: {result['status']}")
            
            # Test reading the Delta table
            df_read = delta_manager.read_table(delta_table_path)
            read_count = df_read.count()
            
            print(f"✅ Delta table read successfully!")
            print(f"   Read {read_count} records")
            
            # Test table health validation
            health = delta_manager.validate_table_health(delta_table_path)
            print(f"✅ Table health check: {health['status']}")
            
            # Test time travel (read version 0)
            df_v0 = delta_manager.read_table(delta_table_path, version=0)
            v0_count = df_v0.count()
            print(f"✅ Time travel read (v0): {v0_count} records")
            
            # Get table history
            history = delta_manager.get_table_history(delta_table_path)
            print(f"✅ Table history: {len(history)} versions")
            
            if history:
                print(f"   Latest operation: {history[0]['operation']}")
            
            spark.stop()
            print("✅ Local Delta table test completed successfully!")
            return True
            
    except Exception as e:
        print(f"❌ Local test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_sample_bronze_data():
    """Test creating sample bronze data that could work with MinIO"""
    print("\n🧪 Testing sample bronze data preparation...")
    
    try:
        from utils.delta_utils import create_delta_spark_session
        from pyspark.sql.functions import current_timestamp, lit, current_date
        
        spark = create_delta_spark_session("TestBronzeData")
        
        # Simulate the bronze token data structure we expect
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
        
        schema = StructType([
            StructField("token_address", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("name", StringType(), True),
            StructField("liquidity", DoubleType(), True),
            StructField("volume_24h_usd", DoubleType(), True),
            StructField("price", DoubleType(), True),
            StructField("price_change_24h_percent", DoubleType(), True),
            StructField("market_cap", DoubleType(), True)
        ])
        
        # Create realistic sample data
        sample_tokens = [
            ("So11111111111111111111111111111111111111112", "SOL", "Solana", 
             1000000.0, 50000000.0, 100.0, 5.2, 50000000000.0),
            ("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "USDC", "USD Coin", 
             2000000.0, 75000000.0, 1.0, 0.1, 35000000000.0),
            ("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "USDT", "Tether USD", 
             1500000.0, 60000000.0, 1.0, -0.05, 45000000000.0)
        ]
        
        df = spark.createDataFrame(sample_tokens, schema)
        
        # Add Delta Lake metadata columns as they would appear in real implementation
        df_with_metadata = df.withColumn("_delta_timestamp", current_timestamp()) \
                            .withColumn("_delta_operation", lit("BRONZE_TOKEN_FETCH")) \
                            .withColumn("processing_date", current_date()) \
                            .withColumn("batch_id", lit("20250624_test"))
        
        print(f"✅ Sample bronze data prepared with {df_with_metadata.count()} tokens")
        print(f"📋 Schema: {len(df_with_metadata.schema)} fields")
        
        # Show sample data
        print("📊 Sample data:")
        df_with_metadata.select("symbol", "liquidity", "volume_24h_usd", "_delta_operation").show(3)
        
        spark.stop()
        print("✅ Sample bronze data test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Sample data test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run simple Delta Lake tests"""
    print("🚀 Simple Delta Lake Test Suite")
    print("=" * 50)
    
    tests = [
        test_local_delta_table,
        test_sample_bronze_data
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📊 Test Results:")
    
    passed = sum(results)
    total = len(results)
    
    print(f"   Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All simple Delta Lake tests passed!")
        print("   Ready to proceed with S3A integration")
        return True
    else:
        print("⚠️  Some tests failed. Fix issues before proceeding.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)