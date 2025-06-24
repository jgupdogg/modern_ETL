"""
Silver Layer Tasks - PROPERLY ARCHITECTED VERSION

Core business logic for silver layer data transformation tasks.
Implements real PnL calculation logic with proper Spark patterns.

Changes:
- Uses date partitioning for efficient reads
- No bronze overwrites - uses separate tracking
- Real PnL calculations using groupBy (no collect!)
- Proper transaction classification (BUY/SELL/SWAP)
"""
import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config

# Import centralized configuration
from config.smart_trader_config import (
    TRACKED_TOKEN_LIMIT, SILVER_MIN_LIQUIDITY, SILVER_MIN_VOLUME,
    SILVER_MIN_VOLUME_MCAP_RATIO, SILVER_MIN_PRICE_CHANGE,
    SOL_TOKEN_ADDRESS, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    SILVER_TRACKED_TOKENS_PATH, SILVER_WALLET_PNL_PATH, BRONZE_WALLET_TRANSACTIONS_PATH,
    # Wallet batch processing parameters
    SILVER_PNL_WALLET_BATCH_SIZE, SILVER_PNL_MAX_TRANSACTIONS_PER_BATCH, SILVER_PNL_PROCESSING_TIMEOUT_SECONDS,
    # Delta Lake configuration
    get_spark_config_with_delta, get_delta_s3_path
)

# PySpark imports (lazy loading)
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, when, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
        current_timestamp, expr, coalesce, round as spark_round,
        datediff, abs as spark_abs
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, 
        TimestampType, BooleanType, DateType
    )
    from delta import DeltaTable
    PYSPARK_AVAILABLE = True
except ImportError as e:
    logging.getLogger(__name__).error(f"PySpark or Delta Lake not available: {e}")
    PYSPARK_AVAILABLE = False


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )


def get_silver_schema() -> pa.Schema:
    """Define PyArrow schema for silver tracked tokens"""
    return pa.schema([
        pa.field('token_address', pa.string()),
        pa.field('symbol', pa.string()),
        pa.field('name', pa.string()),
        pa.field('logoURI', pa.string()),
        pa.field('decimals', pa.int64()),
        pa.field('price', pa.float64()),
        pa.field('liquidity', pa.float64()),
        pa.field('marketcap', pa.float64()),
        pa.field('volume24hUSD', pa.float64()),
        pa.field('volume24hChangePercent', pa.float64()),
        pa.field('price24hChangePercent', pa.float64()),
        pa.field('volume_mcap_ratio', pa.float64()),
        pa.field('rank', pa.int64()),
        pa.field('quality_score', pa.float64()),
        pa.field('created_at', pa.timestamp('us')),
        pa.field('updated_at', pa.timestamp('us')),
        pa.field('processing_date', pa.date32()),
        pa.field('bronze_id', pa.string())
    ])


def transform_silver_tracked_tokens(**context):
    """
    Transform bronze token list data into silver tracked tokens using direct MinIO access
    
    ACTIVE FUNCTION - Used by smart_trader_identification_dag
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Querying bronze token data directly from MinIO")
        
        # Get MinIO client
        s3_client = get_minio_client()
        
        # List bronze token files
        try:
            response = s3_client.list_objects_v2(
                Bucket='solana-data',
                Prefix='bronze/token_list_v3/',
                MaxKeys=1000
            )
            
            if 'Contents' not in response:
                logger.warning("No bronze token data found")
                return {"tokens_processed": 0, "tokens_tracked": 0}
            
            # Filter for parquet files only and get the most recent
            parquet_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
            if not parquet_files:
                logger.warning("No parquet files found in bronze token data")
                return {"tokens_processed": 0, "tokens_tracked": 0}
            
            bronze_files = sorted(parquet_files, key=lambda x: x['LastModified'], reverse=True)
            latest_file = bronze_files[0]['Key']
            
            logger.info(f"Reading latest bronze file: {latest_file}")
            
            # Download and read parquet file
            obj_response = s3_client.get_object(Bucket='solana-data', Key=latest_file)
            parquet_data = obj_response['Body'].read()
            
            # Read parquet from bytes
            table = pq.read_table(BytesIO(parquet_data))
            df = table.to_pandas()
            
            total_bronze_tokens = len(df)
            logger.info(f"Found {total_bronze_tokens} tokens in bronze data")
            
            if total_bronze_tokens == 0:
                return {"tokens_processed": 0, "tokens_tracked": 0}
            
            # Apply filtering logic in pandas
            logger.info("Applying filtering criteria")
            
            # Remove duplicates (keep latest by ingested_at)
            df = df.sort_values('ingested_at', ascending=False).drop_duplicates('token_address', keep='first')
            
            # Apply lenient filters for testing
            mask = (
                (df['token_address'].notna()) &
                (df['liquidity'] >= SILVER_MIN_LIQUIDITY)
            )
            logger.info(f"Applied lenient filters: address + liquidity >= {SILVER_MIN_LIQUIDITY}")
            
            filtered_df = df[mask].copy()
            
            # Calculate volume/mcap ratio and apply ratio filter
            filtered_df['volume_mcap_ratio'] = filtered_df.apply(
                lambda row: row['volume_24h_usd'] / row['market_cap'] 
                if pd.notna(row['market_cap']) and row['market_cap'] > 0 
                else None, axis=1
            )
            
            # Apply volume/mcap ratio filter
            ratio_mask = (
                filtered_df['market_cap'].isna() | 
                (filtered_df['market_cap'] <= 0) |
                (filtered_df['volume_mcap_ratio'] >= SILVER_MIN_VOLUME_MCAP_RATIO)
            )
            filtered_df = filtered_df[ratio_mask]
            
            # Sort and limit
            filtered_df = filtered_df.sort_values('liquidity', ascending=False).head(TRACKED_TOKEN_LIMIT)
            
            # Rename columns to match silver schema
            filtered_df = filtered_df.rename(columns={
                'logo_uri': 'logoURI',
                'market_cap': 'marketcap',
                'volume_24h_usd': 'volume24hUSD',
                'volume_24h_change_percent': 'volume24hChangePercent',
                'price_change_24h_percent': 'price24hChangePercent',
                'batch_id': 'bronze_id'
            })
            
            logger.info(f"Filtered to {len(filtered_df)} tokens that passed criteria")
            
        except Exception as e:
            logger.error(f"Error reading bronze data from MinIO: {e}")
            raise
        
        if len(filtered_df) == 0:
            logger.warning("No tokens passed filtering criteria")
            return {"tokens_processed": total_bronze_tokens, "tokens_tracked": 0}
        
        # Use the filtered DataFrame as our final dataset
        df = filtered_df
        
        # Add calculated fields and metadata
        current_time = pd.Timestamp.utcnow()
        processing_date = current_time.date()
        
        df['rank'] = None  # No rank calculation for now
        df['quality_score'] = None  # No quality score calculation for now
        df['created_at'] = current_time
        df['updated_at'] = current_time
        df['processing_date'] = processing_date
        
        # Ensure schema compliance
        schema = get_silver_schema()
        
        # Reorder columns to match schema
        ordered_columns = [field.name for field in schema]
        df = df[ordered_columns]
        
        logger.info(f"Sample transformed data:\\n{df.head(3)[['token_address', 'symbol', 'volume24hUSD', 'volume_mcap_ratio']]}")
        
        # Convert to PyArrow table with schema
        table = pa.Table.from_pandas(df, schema=schema)
        
        # Prepare MinIO path with date partitioning
        date_partition = processing_date.strftime("processing_date=%Y-%m-%d")
        timestamp = current_time.strftime("%Y%m%d_%H%M%S")
        file_name = f"tracked_tokens_{timestamp}.parquet"
        s3_key = f"silver/tracked_tokens/{date_partition}/{file_name}"
        
        # Write to MinIO
        s3_client = get_minio_client()
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        try:
            s3_client.put_object(
                Bucket='solana-data',
                Key=s3_key,
                Body=buffer.getvalue()
            )
            logger.info(f"Successfully wrote {len(df)} tracked tokens to s3://solana-data/{s3_key}")
            
            # Write success marker
            success_key = f"silver/tracked_tokens/{date_partition}/_SUCCESS"
            s3_client.put_object(
                Bucket='solana-data',
                Key=success_key,
                Body=b''
            )
            
        except Exception as e:
            logger.error(f"Error writing to MinIO: {e}")
            raise
        
        # Return metadata
        return {
            "tokens_processed": total_bronze_tokens,
            "tokens_tracked": len(df),
            "s3_path": f"s3://solana-data/{s3_key}",
            "processing_date": str(processing_date)
        }
        
    except Exception as e:
        logger.error(f"Silver transformation failed: {e}")
        raise


# ===== PySpark-dependent functions (only defined if PySpark is available) =====
if PYSPARK_AVAILABLE:
    
    def get_silver_pnl_schema() -> StructType:
        """Get PySpark schema for silver PnL data"""
        return StructType([
            # Wallet identification
            StructField("wallet_address", StringType(), False),
            StructField("token_address", StringType(), True),  # 'ALL_TOKENS' for portfolio-level
            StructField("calculation_date", DateType(), False),
            
            # Core PnL metrics
            StructField("realized_pnl", DoubleType(), False),
            StructField("unrealized_pnl", DoubleType(), False), 
            StructField("total_pnl", DoubleType(), False),
            
            # Trading metrics
            StructField("trade_count", IntegerType(), False),
            StructField("win_rate", DoubleType(), False),
            StructField("total_bought", DoubleType(), False),
            StructField("total_sold", DoubleType(), False),
            StructField("roi", DoubleType(), False),
            StructField("avg_holding_time_hours", DoubleType(), False),
            StructField("avg_transaction_amount_usd", DoubleType(), False),
            StructField("trade_frequency_daily", DoubleType(), False),
            
            # Time ranges
            StructField("first_transaction", TimestampType(), True),
            StructField("last_transaction", TimestampType(), True),
            
            # Position metrics
            StructField("current_position_tokens", DoubleType(), False),
            StructField("current_position_cost_basis", DoubleType(), False),
            StructField("current_position_value", DoubleType(), False),
            
            # Processing metadata
            StructField("processed_at", TimestampType(), False),
            StructField("batch_id", StringType(), False),
            StructField("data_source", StringType(), False),
        ])


    def get_spark_session_with_s3() -> SparkSession:
        """Create Spark session with Delta Lake support and conservative memory settings"""
        logger = logging.getLogger(__name__)
        
        logger.info("🔧 Creating Spark session with Delta Lake support")
        
        # Configure Spark for Delta Lake using packages that work
        spark = SparkSession.builder \
            .appName("SilverWalletPnL_Delta") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-spark_2.12:3.0.0") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.maxResultSize", "256m") \
            .config("spark.sql.shuffle.partitions", "50") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000") \
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
            .config("spark.sql.parquet.int96AsTimestamp", "true") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session created successfully")
        
        # Configure Delta Lake after session creation (if available)
        try:
            from delta import configure_spark_with_delta_pip
            spark = configure_spark_with_delta_pip(spark)
            logger.info("✅ Delta Lake configured successfully using pip package")
        except ImportError:
            logger.warning("⚠️ Delta Lake pip configuration not available, using basic JAR packages")
            
        return spark


    def check_if_date_processed(s3_client, target_date: str) -> bool:
        """Check if a date has already been processed"""
        logger = logging.getLogger(__name__)
        
        try:
            # Check for processing log files
            prefix = f"silver/wallet_pnl/_processing_log/date={target_date}/"
            response = s3_client.list_objects_v2(
                Bucket='solana-data',
                Prefix=prefix,
                MaxKeys=1
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                logger.info(f"Date {target_date} already processed")
                return True
            else:
                logger.info(f"Date {target_date} not yet processed")
                return False
                
        except Exception as e:
            logger.warning(f"Error checking processing status: {e}")
            return False


    def write_processing_log(s3_client, target_date: str, batch_id: str, result: Dict[str, Any]):
        """Write processing log for tracking"""
        logger = logging.getLogger(__name__)
        
        log_data = {
            "date": target_date,
            "batch_id": batch_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "wallets_processed": result.get("wallets_processed", 0),
            "records_created": result.get("total_pnl_records", 0),
            "status": result.get("status", "unknown")
        }
        
        key = f"silver/wallet_pnl/_processing_log/date={target_date}/batch_{batch_id}.json"
        
        try:
            s3_client.put_object(
                Bucket='solana-data',
                Key=key,
                Body=json.dumps(log_data, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Wrote processing log to {key}")
        except Exception as e:
            logger.error(f"Failed to write processing log: {e}")


    def read_transactions_for_date(spark: SparkSession, target_date: str) -> DataFrame:
        """Read wallet transactions for a specific date partition"""
        logger = logging.getLogger(__name__)
        
        # Read ONLY the specific date partition
        parquet_path = f"s3a://solana-data/{BRONZE_WALLET_TRANSACTIONS_PATH}/date={target_date}/*.parquet"
        
        try:
            bronze_df = spark.read.parquet(parquet_path)
            
            # Transform to expected schema and calculate USD values
            transformed_df = bronze_df.select(
                col("wallet_address"),
                # Use base_address as token_address (the token being traded)
                col("base_address").alias("token_address"),
                col("base_address").alias("from_token"),
                col("quote_address").alias("to_token"),
                # Calculate amounts (use absolute values)
                spark_abs(col("base_ui_change_amount")).alias("from_amount"),
                spark_abs(col("quote_ui_change_amount")).alias("to_amount"),
                # Calculate USD value using nearest prices
                (spark_abs(col("base_ui_change_amount")) * coalesce(col("base_nearest_price"), lit(0.0))).alias("value_usd"),
                col("timestamp"),
                col("transaction_hash")
            )
            
            # Basic quality filters
            clean_df = transformed_df.filter(
                (col("transaction_hash").isNotNull()) &
                (col("wallet_address").isNotNull()) &
                (col("timestamp").isNotNull()) &
                (col("value_usd").isNotNull()) &
                (col("value_usd") > 0) &
                (col("from_amount").isNotNull()) &
                (col("to_amount").isNotNull())
            )
            
            return clean_df
            
        except Exception as e:
            logger.warning(f"No data found for date {target_date}: {e}")
            # Return empty DataFrame with expected schema
            schema = StructType([
                StructField("wallet_address", StringType()),
                StructField("token_address", StringType()),
                StructField("from_token", StringType()),
                StructField("to_token", StringType()),
                StructField("from_amount", DoubleType()),
                StructField("to_amount", DoubleType()),
                StructField("value_usd", DoubleType()),
                StructField("timestamp", TimestampType()),
                StructField("transaction_hash", StringType()),
            ])
            return spark.createDataFrame([], schema)


    def calculate_wallet_pnl_optimized(transactions_df: DataFrame, batch_id: str) -> DataFrame:
        """
        Calculate real PnL metrics using proper Spark patterns (no collect!)
        Uses groupBy for parallel processing
        """
        logger = logging.getLogger(__name__)
        
        # Step 1: Classify transactions as BUY/SELL/SWAP
        classified_df = transactions_df.withColumn(
            "trade_type",
            when(col("from_token") == SOL_TOKEN_ADDRESS, "BUY")
            .when(col("to_token") == SOL_TOKEN_ADDRESS, "SELL")
            .otherwise("SWAP")
        )
        
        # Step 2: Calculate metrics per wallet-token pair
        wallet_token_metrics = classified_df.groupBy("wallet_address", "token_address").agg(
            # Volume metrics
            spark_sum(when(col("trade_type") == "BUY", col("value_usd")).otherwise(0)).alias("total_bought_usd"),
            spark_sum(when(col("trade_type") == "SELL", col("value_usd")).otherwise(0)).alias("total_sold_usd"),
            spark_sum(when(col("trade_type") == "BUY", col("to_amount")).otherwise(0)).alias("tokens_bought"),
            spark_sum(when(col("trade_type") == "SELL", col("from_amount")).otherwise(0)).alias("tokens_sold"),
            
            # Trade counts
            count(when(col("trade_type") == "BUY", 1)).alias("buy_count"),
            count(when(col("trade_type") == "SELL", 1)).alias("sell_count"),
            count("*").alias("total_trades"),
            
            # Price metrics
            avg(when(col("trade_type") == "BUY", col("value_usd") / col("to_amount"))).alias("avg_buy_price"),
            avg(when(col("trade_type") == "SELL", col("value_usd") / col("from_amount"))).alias("avg_sell_price"),
            
            # Timing
            spark_min("timestamp").alias("first_trade"),
            spark_max("timestamp").alias("last_trade")
        )
        
        # Step 3: Calculate PnL and position metrics
        token_pnl_df = wallet_token_metrics.withColumn(
            "tokens_remaining", 
            col("tokens_bought") - col("tokens_sold")
        ).withColumn(
            "realized_pnl",
            col("total_sold_usd") - (col("tokens_sold") * col("avg_buy_price"))
        ).withColumn(
            "current_position_cost_basis",
            col("tokens_remaining") * col("avg_buy_price")
        ).withColumn(
            "current_position_value",
            col("tokens_remaining") * coalesce(col("avg_sell_price"), col("avg_buy_price"))
        ).withColumn(
            "unrealized_pnl",
            col("current_position_value") - col("current_position_cost_basis")
        ).withColumn(
            "total_pnl",
            col("realized_pnl") + col("unrealized_pnl")
        ).withColumn(
            "roi",
            when(col("total_bought_usd") > 0, 
                 (col("total_pnl") / col("total_bought_usd")) * 100
            ).otherwise(0)
        ).withColumn(
            "trade_count",
            col("total_trades")
        )
        
        # Step 4: Portfolio-level aggregation
        portfolio_df = token_pnl_df.groupBy("wallet_address").agg(
            spark_sum("realized_pnl").alias("realized_pnl"),
            spark_sum("unrealized_pnl").alias("unrealized_pnl"),
            spark_sum("total_pnl").alias("total_pnl"),
            spark_sum("total_bought_usd").alias("total_bought"),
            spark_sum("total_sold_usd").alias("total_sold"),
            spark_sum("trade_count").alias("trade_count"),
            avg("roi").alias("roi"),
            spark_min("first_trade").alias("first_transaction"),
            spark_max("last_trade").alias("last_transaction"),
            spark_sum("current_position_cost_basis").alias("current_position_cost_basis"),
            spark_sum("current_position_value").alias("current_position_value"),
            # Win rate: percentage of tokens with positive PnL
            (spark_sum(when(col("total_pnl") > 0, 1).otherwise(0)) * 100.0 / count("*")).alias("win_rate")
        )
        
        # Step 5: Add metadata and calculate derived metrics
        current_time = datetime.now(timezone.utc)
        
        final_df = portfolio_df.withColumn(
            "token_address", lit("ALL_TOKENS")
        ).withColumn(
            "calculation_date", lit(current_time.date())
        ).withColumn(
            "current_position_tokens", lit(0.0)  # Portfolio level, not applicable
        ).withColumn(
            "avg_holding_time_hours",
            (datediff(col("last_transaction"), col("first_transaction")) * 24.0)
        ).withColumn(
            "avg_transaction_amount_usd",
            (col("total_bought") + col("total_sold")) / col("trade_count")
        ).withColumn(
            "trade_frequency_daily",
            col("trade_count") / spark_abs(datediff(col("last_transaction"), col("first_transaction")) + 1)
        ).withColumn(
            "processed_at", lit(current_time)
        ).withColumn(
            "batch_id", lit(batch_id)
        ).withColumn(
            "data_source", lit("optimized_groupby")
        )
        
        # Round numeric values
        for col_name in ["realized_pnl", "unrealized_pnl", "total_pnl", "total_bought", 
                         "total_sold", "roi", "win_rate", "avg_transaction_amount_usd"]:
            final_df = final_df.withColumn(col_name, spark_round(col(col_name), 2))
        
        return final_df


    def write_silver_pnl_data(spark: SparkSession, pnl_df: DataFrame, batch_id: str) -> str:
        """Write PnL results to silver layer using Delta Lake ONLY"""
        logger = logging.getLogger(__name__)
        
        # Add partition columns
        partitioned_df = pnl_df.withColumn(
            "calculation_year", expr("year(calculation_date)")
        ).withColumn(
            "calculation_month", expr("month(calculation_date)")
        )
        
        # Delta Lake path
        delta_output_path = get_delta_s3_path("silver/wallet_pnl")
        
        try:
            # Check if Delta table exists and use MERGE
            try:
                deltaTable = DeltaTable.forPath(spark, delta_output_path)
                
                # Use MERGE to prevent duplicates and handle updates
                deltaTable.alias("target").merge(
                    partitioned_df.alias("source"),
                    "target.wallet_address = source.wallet_address AND " +
                    "target.token_address = source.token_address AND " +
                    "target.calculation_date = source.calculation_date"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
                
                logger.info(f"✅ Successfully merged PnL data to Delta table: {delta_output_path}")
                return delta_output_path
                
            except Exception as delta_error:
                # If table doesn't exist or MERGE fails, create new table
                logger.info(f"Delta MERGE failed ({delta_error}), creating new Delta table")
                partitioned_df.coalesce(1).write \
                    .format("delta") \
                    .partitionBy("calculation_year", "calculation_month") \
                    .mode("append") \
                    .save(delta_output_path)
                
                logger.info(f"✅ Successfully created/appended PnL data to Delta table: {delta_output_path}")
                return delta_output_path
            
        except Exception as delta_error:
            logger.error(f"❌ Delta Lake write FAILED: {delta_error}")
            logger.error(f"❌ Batch {batch_id} could not be written to Delta Lake")
            logger.error("❌ NO FALLBACK - Data processing aborted")
            
            # Import traceback for detailed error information
            import traceback
            logger.error(f"❌ Full Delta Lake error traceback:\n{traceback.format_exc()}")
            
            # Re-raise the exception to fail the task
            raise RuntimeError(f"Delta Lake write failed for batch {batch_id}: {delta_error}")


def transform_silver_wallet_pnl(**context):
    """
    Calculate wallet PnL metrics using optimized PySpark patterns
    
    ACTIVE FUNCTION - Used by smart_trader_identification_dag and test_silver_pnl_dag
    Processes transactions by date partition with real PnL calculations
    UPDATED: Processes last 7 days of transactions
    """
    logger = logging.getLogger(__name__)
    
    if not PYSPARK_AVAILABLE:
        logger.error("PySpark not available")
        return {
            "wallets_processed": 0,
            "total_pnl_records": 0,
            "batch_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            "status": "pyspark_not_available"
        }
    
    # Generate batch ID
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    # Process last 7 days instead of just today
    target_dates = []
    for days_ago in range(7):
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        target_dates.append(date)
    
    logger.info(f"Processing last 7 days: {target_dates}")
    
    # Initialize Spark session once for all dates
    spark = get_spark_session_with_s3()
    s3_client = get_minio_client()
    
    total_wallets_processed = 0
    total_records_created = 0
    dates_processed = []
    dates_skipped = []
    
    try:
        # Process each date
        for target_date in target_dates:
            logger.info(f"Processing date: {target_date}")
            
            # Check if already processed
            if check_if_date_processed(s3_client, target_date):
                logger.info(f"Date {target_date} already processed, skipping")
                dates_skipped.append(target_date)
                continue
            
            # Read transactions for specific date
            transactions_df = read_transactions_for_date(spark, target_date)
            
            # Check if empty using more efficient method
            if transactions_df.rdd.isEmpty():
                logger.info(f"No transactions found for date {target_date}")
                result = {
                    "wallets_processed": 0,
                    "total_pnl_records": 0,
                    "batch_id": batch_id,
                    "status": "no_data",
                    "target_date": target_date
                }
                write_processing_log(s3_client, target_date, batch_id, result)
                continue
            
            # Calculate PnL using optimized groupBy approach
            pnl_df = calculate_wallet_pnl_optimized(transactions_df, batch_id)
            
            # Cache for reuse
            pnl_df.cache()
            
            # Get count efficiently (after cache)
            wallet_count = pnl_df.count()
            
            if wallet_count > 0:
                # Write to silver layer
                output_path = write_silver_pnl_data(spark, pnl_df, batch_id)
                
                logger.info(f"Date {target_date}: Processed {wallet_count} wallets")
                
                total_wallets_processed += wallet_count
                total_records_created += wallet_count
                dates_processed.append(target_date)
                
                result = {
                    "wallets_processed": wallet_count,
                    "total_pnl_records": wallet_count,
                    "batch_id": batch_id,
                    "output_path": output_path,
                    "status": "success",
                    "target_date": target_date
                }
            else:
                logger.warning(f"Date {target_date}: No PnL results generated")
                result = {
                    "wallets_processed": 0,
                    "total_pnl_records": 0,
                    "batch_id": batch_id,
                    "status": "no_results",
                    "target_date": target_date
                }
            
            # Write processing log for this date
            write_processing_log(s3_client, target_date, batch_id, result)
            
            # Unpersist cache to free memory
            pnl_df.unpersist()
        
        # Summary
        logger.info(f"✅ 7-day processing completed!")
        logger.info(f"  Dates processed: {len(dates_processed)} - {dates_processed}")
        logger.info(f"  Dates skipped: {len(dates_skipped)} - {dates_skipped}")
        logger.info(f"  Total wallets: {total_wallets_processed}")
        logger.info(f"  Total records: {total_records_created}")
        
        return {
            "wallets_processed": total_wallets_processed,
            "total_pnl_records": total_records_created,
            "batch_id": batch_id,
            "status": "success",
            "dates_processed": dates_processed,
            "dates_skipped": dates_skipped,
            "processing_summary": f"Processed {len(dates_processed)} dates, skipped {len(dates_skipped)}"
        }
            
    except Exception as e:
        logger.error(f"PySpark processing failed: {e}")
        return {
            "wallets_processed": total_wallets_processed,
            "total_pnl_records": total_records_created,
            "batch_id": batch_id,
            "status": "failed",
            "error": str(e),
            "dates_processed": dates_processed,
            "dates_skipped": dates_skipped
        }
    finally:
        spark.stop()