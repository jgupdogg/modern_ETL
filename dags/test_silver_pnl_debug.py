"""
Test Silver PnL Debug DAG
Simple DAG to test just the silver wallet PnL task and debug why we're getting zero processed items
"""

import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Import centralized configuration
from config.smart_trader_config import (
    get_spark_config,
    SILVER_PNL_RECENT_DAYS, SILVER_PNL_HISTORICAL_LIMIT, 
    SILVER_PNL_MIN_TRANSACTIONS, PNL_AMOUNT_PRECISION_THRESHOLD,
    PNL_BATCH_PROGRESS_INTERVAL, BRONZE_WALLET_TRANSACTIONS_PATH
)

# DAG definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'test_silver_pnl_debug',
    default_args=default_args,
    description='Debug silver wallet PnL processing',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'debug', 'silver', 'pnl']
)

@task(dag=dag)
def debug_bronze_data_analysis(**context):
    """Analyze bronze data to understand what we have"""
    logger = logging.getLogger(__name__)
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, countDistinct, desc, asc
        
        logger.info("=== BRONZE DATA DEBUG ANALYSIS ===")
        
        # Create Spark session
        spark_config = get_spark_config()
        spark = SparkSession.builder \
            .appName("DebugBronzeAnalysis") \
            .config("spark.jars.packages", spark_config["spark.jars.packages"]) \
            .config("spark.hadoop.fs.s3a.endpoint", spark_config["spark.hadoop.fs.s3a.endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", spark_config["spark.hadoop.fs.s3a.access.key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", spark_config["spark.hadoop.fs.s3a.secret.key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        try:
            # Read bronze data with clean files only
            parquet_path = f"s3a://solana-data/{BRONZE_WALLET_TRANSACTIONS_PATH}/**/clean_*.parquet"
            logger.info(f"Reading from path: {parquet_path}")
            
            bronze_df = spark.read.parquet(parquet_path)
            logger.info("✅ Successfully read bronze data")
            
            # Basic statistics
            total_records = bronze_df.count()
            logger.info(f"📊 Total records: {total_records:,}")
            
            if total_records == 0:
                logger.error("❌ PROBLEM: No records found in bronze data!")
                return {"status": "error", "message": "No bronze data found"}
            
            # Schema analysis
            schema_fields = bronze_df.schema.fieldNames()
            logger.info(f"📋 Schema fields ({len(schema_fields)}): {schema_fields}")
            
            # Check for required columns
            required_cols = ['tx_type', 'wallet_address', 'transaction_hash', 'timestamp', 'base_address', 'quote_address']
            missing_cols = [col for col in required_cols if col not in schema_fields]
            if missing_cols:
                logger.error(f"❌ PROBLEM: Missing required columns: {missing_cols}")
                return {"status": "error", "message": f"Missing columns: {missing_cols}"}
            else:
                logger.info("✅ All required columns present")
            
            # Transaction type analysis
            tx_type_counts = bronze_df.groupBy("tx_type").count().orderBy(desc("count")).collect()
            logger.info("📈 Transaction types:")
            for row in tx_type_counts:
                logger.info(f"  {row['tx_type']}: {row['count']:,}")
            
            # Check swap transactions specifically
            swap_df = bronze_df.filter(col("tx_type") == "swap")
            swap_count = swap_df.count()
            logger.info(f"🔄 Swap transactions: {swap_count:,}")
            
            if swap_count == 0:
                logger.error("❌ PROBLEM: No swap transactions found!")
                return {"status": "error", "message": "No swap transactions"}
            
            # Wallet analysis
            unique_wallets = bronze_df.select("wallet_address").distinct().count()
            swap_wallets = swap_df.select("wallet_address").distinct().count()
            logger.info(f"👛 Total unique wallets: {unique_wallets:,}")
            logger.info(f"👛 Wallets with swaps: {swap_wallets:,}")
            
            # Check for nulls in critical fields
            null_checks = {}
            for col_name in required_cols:
                null_count = bronze_df.filter(col(col_name).isNull()).count()
                null_checks[col_name] = null_count
                if null_count > 0:
                    logger.warning(f"⚠️  {col_name} has {null_count:,} null values")
                else:
                    logger.info(f"✅ {col_name} has no nulls")
            
            # Sample data
            logger.info("📋 Sample records:")
            sample_data = bronze_df.limit(5).collect()
            for i, row in enumerate(sample_data):
                logger.info(f"  Record {i+1}: wallet={row['wallet_address'][:20]}..., tx_type={row['tx_type']}, hash={row['transaction_hash'][:20]}...")
            
            return {
                "status": "success",
                "total_records": total_records,
                "swap_transactions": swap_count,
                "unique_wallets": unique_wallets,
                "swap_wallets": swap_wallets,
                "tx_type_counts": {row['tx_type']: row['count'] for row in tx_type_counts},
                "null_checks": null_checks,
                "schema_fields": schema_fields
            }
            
        finally:
            spark.stop()
            
    except Exception as e:
        logger.error(f"❌ Bronze data analysis failed: {e}")
        return {"status": "error", "message": str(e)}

@task(dag=dag)
def debug_silver_pnl_logic(**context):
    """Debug the actual silver PnL logic step by step"""
    logger = logging.getLogger(__name__)
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit
        from datetime import datetime, timezone, timedelta
        
        logger.info("=== SILVER PNL LOGIC DEBUG ===")
        
        # Create Spark session
        spark_config = get_spark_config()
        spark = SparkSession.builder \
            .appName("DebugSilverPnL") \
            .config("spark.jars.packages", spark_config["spark.jars.packages"]) \
            .config("spark.hadoop.fs.s3a.endpoint", spark_config["spark.hadoop.fs.s3a.endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", spark_config["spark.hadoop.fs.s3a.access.key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", spark_config["spark.hadoop.fs.s3a.secret.key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        try:
            # Read bronze data
            parquet_path = f"s3a://solana-data/{BRONZE_WALLET_TRANSACTIONS_PATH}/**/clean_*.parquet"
            bronze_df = spark.read.parquet(parquet_path)
            logger.info(f"✅ Read {bronze_df.count():,} bronze records")
            
            # Step 1: Deduplicate
            deduped_df = bronze_df.dropDuplicates(["wallet_address", "transaction_hash"])
            deduped_count = deduped_df.count()
            logger.info(f"📊 After deduplication: {deduped_count:,} records")
            
            # Step 2: Filter for swaps with required fields
            logger.info("🔍 Applying filters...")
            
            # Check each filter step by step
            filter_step1 = deduped_df.filter(col("tx_type") == "swap")
            step1_count = filter_step1.count()
            logger.info(f"  After tx_type = 'swap': {step1_count:,}")
            
            filter_step2 = filter_step1.filter(col("transaction_hash").isNotNull())
            step2_count = filter_step2.count()
            logger.info(f"  After transaction_hash not null: {step2_count:,}")
            
            filter_step3 = filter_step2.filter(col("wallet_address").isNotNull())
            step3_count = filter_step3.count()
            logger.info(f"  After wallet_address not null: {step3_count:,}")
            
            filter_step4 = filter_step3.filter(col("timestamp").isNotNull())
            step4_count = filter_step4.count()
            logger.info(f"  After timestamp not null: {step4_count:,}")
            
            filter_step5 = filter_step4.filter(col("base_address").isNotNull())
            step5_count = filter_step5.count()
            logger.info(f"  After base_address not null: {step5_count:,}")
            
            swap_df = filter_step5.filter(col("quote_address").isNotNull())
            final_count = swap_df.count()
            logger.info(f"  After quote_address not null: {final_count:,}")
            
            if final_count == 0:
                logger.error("❌ PROBLEM: No records after filtering!")
                
                # Debug which filter is eliminating all records
                logger.info("🔍 Debugging filter eliminations...")
                
                # Check for swap transactions
                swap_check = deduped_df.filter(col("tx_type") == "swap").count()
                if swap_check == 0:
                    # Check what tx_type values we have
                    tx_types = deduped_df.select("tx_type").distinct().collect()
                    logger.error(f"❌ No 'swap' tx_type found. Available types: {[row['tx_type'] for row in tx_types]}")
                
                # Check for null addresses
                base_nulls = deduped_df.filter(col("base_address").isNull()).count()
                quote_nulls = deduped_df.filter(col("quote_address").isNull()).count()
                logger.info(f"📊 Null base_address: {base_nulls:,}, null quote_address: {quote_nulls:,}")
                
                return {"status": "error", "message": "No records pass filters"}
            
            # Step 3: Get unique wallets
            unique_wallets = swap_df.select("wallet_address").distinct().collect()
            wallet_count = len(unique_wallets)
            logger.info(f"👛 Found {wallet_count} unique wallets with valid swap transactions")
            
            if wallet_count == 0:
                logger.error("❌ PROBLEM: No wallets found after filtering!")
                return {"status": "error", "message": "No wallets found"}
            
            # Step 4: Check transaction processing logic
            logger.info("🔍 Checking transaction processing logic...")
            
            current_time = datetime.now(timezone.utc)
            week_ago = current_time - timedelta(days=SILVER_PNL_RECENT_DAYS)
            
            # Test first wallet
            test_wallet = unique_wallets[0]['wallet_address']
            logger.info(f"🧪 Testing wallet: {test_wallet}")
            
            wallet_txns = swap_df.filter(col("wallet_address") == test_wallet)
            wallet_txn_count = wallet_txns.count()
            logger.info(f"  Wallet transactions: {wallet_txn_count}")
            
            # Apply time filtering
            recent_txns = wallet_txns.filter(col("timestamp") >= lit(week_ago))
            recent_count = recent_txns.count()
            logger.info(f"  Recent transactions (last {SILVER_PNL_RECENT_DAYS} days): {recent_count}")
            
            if recent_count < SILVER_PNL_HISTORICAL_LIMIT:
                older_txns = wallet_txns.filter(col("timestamp") < lit(week_ago)) \
                    .orderBy(col("timestamp").desc()) \
                    .limit(SILVER_PNL_HISTORICAL_LIMIT - recent_count)
                older_count = older_txns.count()
                logger.info(f"  Adding older transactions: {older_count}")
                final_txns = recent_txns.unionAll(older_txns)
            else:
                final_txns = recent_txns
            
            final_txn_count = final_txns.count()
            logger.info(f"  Final transactions for processing: {final_txn_count}")
            
            if final_txn_count < SILVER_PNL_MIN_TRANSACTIONS:
                logger.warning(f"⚠️  Wallet has {final_txn_count} < {SILVER_PNL_MIN_TRANSACTIONS} min transactions, would be skipped")
            else:
                logger.info(f"✅ Wallet qualifies for processing ({final_txn_count} >= {SILVER_PNL_MIN_TRANSACTIONS})")
            
            return {
                "status": "success",
                "total_records": deduped_count,
                "swap_records": final_count,
                "unique_wallets": wallet_count,
                "test_wallet": test_wallet,
                "test_wallet_txns": final_txn_count,
                "min_txn_threshold": SILVER_PNL_MIN_TRANSACTIONS
            }
            
        finally:
            spark.stop()
            
    except Exception as e:
        logger.error(f"❌ Silver PnL logic debug failed: {e}")
        return {"status": "error", "message": str(e)}

# Define task dependencies
debug_bronze_data_analysis() >> debug_silver_pnl_logic()