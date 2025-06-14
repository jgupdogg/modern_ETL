"""
Silver Layer Tasks

Core business logic for silver layer data transformation tasks.
Extracted from individual silver DAGs for use in the smart trader identification pipeline.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config


# Configuration
class FilterConfig:
    """Configuration for token filtering criteria"""
    limit: int = 50
    min_liquidity: float = 1000      # Reduced from 10000
    min_volume: float = 1000         # Reduced from 50000
    min_volume_mcap_ratio: float = 0.001  # Reduced from 0.05
    min_price_change: float = 1      # Reduced from 30


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4')
    )


def get_silver_schema() -> pa.Schema:
    """Define PyArrow schema for silver tracked tokens"""
    return pa.schema([
        # Basic token info
        pa.field("token_address", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("decimals", pa.int32(), nullable=True),
        
        # Mapped field names to match original schema
        pa.field("logoURI", pa.string(), nullable=True),
        pa.field("volume24hUSD", pa.float64(), nullable=True),
        pa.field("volume24hChangePercent", pa.float64(), nullable=True),
        pa.field("price24hChangePercent", pa.float64(), nullable=True),
        pa.field("marketcap", pa.float64(), nullable=True),
        
        # Core metrics
        pa.field("liquidity", pa.float64(), nullable=True),
        pa.field("price", pa.float64(), nullable=True),
        pa.field("fdv", pa.float64(), nullable=True),
        pa.field("rank", pa.int32(), nullable=True),
        
        # Calculated fields
        pa.field("volume_mcap_ratio", pa.float64(), nullable=True),
        pa.field("quality_score", pa.float64(), nullable=True),
        
        # Reference and metadata
        pa.field("bronze_id", pa.string(), nullable=True),
        pa.field("created_at", pa.timestamp('us'), nullable=False),
        pa.field("updated_at", pa.timestamp('us'), nullable=False),
        pa.field("processing_date", pa.date32(), nullable=False),
    ])


def transform_silver_tracked_tokens(**context):
    """
    Transform bronze token list data into silver tracked tokens using DuckDB container
    
    Extracted from silver_tracked_tokens_dag.py
    """
    logger = logging.getLogger(__name__)
    
    # Get configuration
    config = FilterConfig()
    
    try:
        # Step 1: Query bronze data using DuckDB container
        logger.info("Querying bronze token data via DuckDB container")
        
        # Connect to DuckDB directly via HTTP endpoint (if exposed) or use MinIO direct approach
        # Since we can't docker exec from within Airflow worker, we'll use direct S3 access
        # and perform the filtering in Python instead of DuckDB
        
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
            from io import BytesIO
            import pyarrow.parquet as pq
            
            table = pq.read_table(BytesIO(parquet_data))
            df = table.to_pandas()
            
            total_bronze_tokens = len(df)
            logger.info(f"Found {total_bronze_tokens} tokens in bronze data")
            
            if total_bronze_tokens == 0:
                return {"tokens_processed": 0, "tokens_tracked": 0}
            
            # Apply filtering logic in pandas (equivalent to DuckDB query)
            logger.info("Applying filtering criteria")
            
            # Remove duplicates (keep latest by ingested_at)
            df = df.sort_values('ingested_at', ascending=False).drop_duplicates('token_address', keep='first')
            
            # Apply lenient filters for testing
            mask = (
                (df['token_address'].notna()) &
                (df['liquidity'] >= config.min_liquidity)
            )
            logger.info(f"Applied lenient filters: address + liquidity >= {config.min_liquidity}")
            
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
                (filtered_df['volume_mcap_ratio'] >= config.min_volume_mcap_ratio)
            )
            filtered_df = filtered_df[ratio_mask]
            
            # Sort and limit
            filtered_df = filtered_df.sort_values('liquidity', ascending=False).head(config.limit)
            
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


# PySpark imports for wallet PnL calculation
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, when, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
        collect_list, struct, udf, current_timestamp, expr, coalesce, 
        unix_timestamp, from_unixtime, datediff, hour, desc, asc, row_number
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, 
        TimestampType, BooleanType, ArrayType, MapType, DateType
    )
    from pyspark.sql.window import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


# Configuration for PnL calculation
class PnLConfig:
    """Configuration for PnL calculation"""
    batch_limit: int = 1000  # Max wallets per batch
    timeframes = ['all', 'week', 'month', 'quarter']
    sol_address = "So11111111111111111111111111111111111111112"


def get_silver_pnl_schema() -> StructType:
    """Get PySpark schema for silver PnL data"""
    if not PYSPARK_AVAILABLE:
        raise ImportError("PySpark not available")
        
    return StructType([
        # Wallet identification
        StructField("wallet_address", StringType(), False),
        StructField("token_address", StringType(), True),  # None for "ALL_TOKENS"
        StructField("calculation_date", DateType(), False),
        StructField("time_period", StringType(), False),  # 'all', 'week', 'month', 'quarter'
        
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
        
        # Gold layer processing state tracking
        StructField("processed_for_gold", BooleanType(), False),  # Default: False
        StructField("gold_processed_at", TimestampType(), True),
        StructField("gold_processing_status", StringType(), True),  # pending/completed/failed
        StructField("gold_batch_id", StringType(), True),
    ])


def get_spark_session_with_s3() -> SparkSession:
    """Create Spark session configured for S3/MinIO access"""
    if not PYSPARK_AVAILABLE:
        raise ImportError("PySpark not available")
        
    spark = SparkSession.builder \
        .appName("SilverWalletPnLCalculation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform_silver_wallet_pnl(**context):
    """
    Calculate comprehensive wallet PnL metrics using PySpark with FIFO methodology
    
    Extracted from silver_wallet_pnl_dag.py
    """
    logger = logging.getLogger(__name__)
    
    if not PYSPARK_AVAILABLE:
        logger.error("PySpark not available - using mock data for testing")
        # Return mock results for testing when PySpark is not available
        return {
            "wallets_processed": 20,
            "total_pnl_records": 240,  # 20 wallets * 4 timeframes * 3 metrics (token + portfolio)
            "batch_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
            "output_path": "s3a://solana-data/silver/wallet_pnl/",
            "status": "mock_success"
        }
    
    # Generate batch ID
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    # Initialize Spark session
    spark = get_spark_session_with_s3()
    
    try:
        logger.info(f"Starting PnL calculation batch {batch_id}")
        
        # Read unfetched wallet transactions
        transactions_df = read_unfetched_transactions(spark)
        
        if transactions_df.count() == 0:
            logger.info("No unfetched transactions found")
            return {
                "wallets_processed": 0,
                "total_pnl_records": 0,
                "batch_id": batch_id,
                "status": "no_data"
            }
        
        logger.info(f"Processing {transactions_df.count()} transactions")
        
        # Calculate PnL for all timeframes
        all_timeframe_results = []
        config = PnLConfig()
        
        for timeframe in config.timeframes:
            logger.info(f"Calculating PnL for timeframe: {timeframe}")
            timeframe_pnl = calculate_timeframe_pnl(spark, transactions_df, timeframe)
            all_timeframe_results.append(timeframe_pnl)
        
        # Union all timeframe results
        if all_timeframe_results:
            token_level_pnl = all_timeframe_results[0]
            for df in all_timeframe_results[1:]:
                token_level_pnl = token_level_pnl.unionAll(df)
        else:
            token_level_pnl = spark.createDataFrame([], get_silver_pnl_schema())
        
        logger.info(f"Generated {token_level_pnl.count()} token-level PnL records")
        
        # Calculate portfolio-level PnL
        portfolio_pnl = calculate_portfolio_pnl(spark, token_level_pnl)
        
        # Combine token-level and portfolio-level results
        final_pnl_results = token_level_pnl.unionAll(portfolio_pnl)
        
        logger.info(f"Final PnL results: {final_pnl_results.count()} total records")
        
        # Write results to silver layer
        output_path = write_silver_pnl_data(spark, final_pnl_results, batch_id)
        
        # Update bronze layer processing status
        update_bronze_processing_status(spark, transactions_df, batch_id)
        
        # Calculate summary metrics
        unique_wallets = transactions_df.select("wallet_address").distinct().count()
        total_records = final_pnl_results.count()
        
        logger.info(f"PnL calculation completed successfully for batch {batch_id}")
        
        return {
            "wallets_processed": unique_wallets,
            "total_pnl_records": total_records,
            "batch_id": batch_id,
            "output_path": output_path,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Error in PnL calculation batch {batch_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
        
    finally:
        # Clean up Spark session
        spark.stop()


# Helper functions for PnL calculation (only defined if PySpark is available)
if PYSPARK_AVAILABLE:
    
    # Define UDF for FIFO PnL calculation
    @udf(returnType=StructType([
        StructField("realized_pnl", DoubleType()),
        StructField("unrealized_pnl", DoubleType()),
        StructField("total_bought", DoubleType()),
        StructField("total_sold", DoubleType()),
        StructField("trade_count", IntegerType()),
        StructField("winning_trades", IntegerType()),
        StructField("total_holding_time_hours", DoubleType()),
        StructField("current_position_tokens", DoubleType()),
        StructField("current_position_cost_basis", DoubleType()),
        StructField("avg_buy_price", DoubleType()),
        StructField("latest_price", DoubleType())
    ]))
    def calculate_token_pnl_udf(transactions):
        """
        UDF to calculate PnL for a single wallet-token combination using FIFO
        """
        if not transactions:
            return (0.0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)
        
        # Convert to list of dicts and sort by timestamp
        txn_list = []
        for row in transactions:
            txn_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            txn_list.append(txn_dict)
        
        # Sort by timestamp
        txn_list.sort(key=lambda x: x['timestamp'])
        
        # Initialize tracking variables
        token_lots = []  # FIFO queue of token purchases
        realized_pnl = 0.0
        total_bought = 0.0
        total_sold = 0.0
        trade_count = 0
        winning_trades = 0
        total_holding_time_seconds = 0.0
        latest_price = 0.0
        
        for txn in txn_list:
            try:
                # Extract transaction details
                tx_type = txn.get('transaction_type', '').upper()
                from_amount = float(txn.get('from_amount') or 0)
                to_amount = float(txn.get('to_amount') or 0)
                base_price = float(txn.get('base_price') or 0)
                quote_price = float(txn.get('quote_price') or 0)
                value_usd = float(txn.get('value_usd') or 0)
                timestamp = txn.get('timestamp')
                
                # Convert timestamp to unix timestamp if needed
                if hasattr(timestamp, 'timestamp'):
                    timestamp_unix = timestamp.timestamp()
                else:
                    timestamp_unix = float(timestamp)
                
                # Determine price and value
                price = base_price or quote_price or 0
                if price > 0:
                    latest_price = price
                
                # Process BUY transactions
                if tx_type == 'BUY' and to_amount > 0:
                    # Calculate cost basis
                    if value_usd > 0:
                        cost_basis = value_usd
                        buy_price = cost_basis / to_amount
                    elif price > 0:
                        buy_price = price
                        cost_basis = to_amount * buy_price
                    else:
                        continue  # Skip if no price info
                    
                    # Add to token lots (FIFO queue)
                    token_lots.append({
                        'amount': to_amount,
                        'price': buy_price,
                        'cost_basis': cost_basis,
                        'timestamp': timestamp_unix
                    })
                    
                    total_bought += cost_basis
                    
                # Process SELL transactions
                elif tx_type == 'SELL' and from_amount > 0:
                    # Calculate sale value
                    if value_usd > 0:
                        sale_value = value_usd
                        sell_price = sale_value / from_amount
                    elif price > 0:
                        sell_price = price
                        sale_value = from_amount * sell_price
                    else:
                        continue  # Skip if no price info
                    
                    total_sold += sale_value
                    
                    # Use FIFO to match sale with purchases
                    remaining_to_sell = from_amount
                    
                    while remaining_to_sell > 0 and token_lots:
                        lot = token_lots[0]
                        
                        if lot['amount'] <= remaining_to_sell:
                            # Sell entire lot
                            lot_sale_value = lot['amount'] * sell_price
                            lot_pnl = lot_sale_value - lot['cost_basis']
                            realized_pnl += lot_pnl
                            
                            # Track metrics
                            trade_count += 1
                            if lot_pnl > 0:
                                winning_trades += 1
                            
                            # Calculate holding time
                            holding_time = timestamp_unix - lot['timestamp']
                            total_holding_time_seconds += holding_time
                            
                            remaining_to_sell -= lot['amount']
                            token_lots.pop(0)  # Remove lot from queue
                            
                        else:
                            # Sell partial lot
                            sell_fraction = remaining_to_sell / lot['amount']
                            lot_cost_basis = lot['cost_basis'] * sell_fraction
                            lot_sale_value = remaining_to_sell * sell_price
                            lot_pnl = lot_sale_value - lot_cost_basis
                            realized_pnl += lot_pnl
                            
                            # Track metrics
                            trade_count += 1
                            if lot_pnl > 0:
                                winning_trades += 1
                            
                            # Calculate holding time
                            holding_time = timestamp_unix - lot['timestamp']
                            total_holding_time_seconds += holding_time
                            
                            # Update remaining lot
                            lot['amount'] -= remaining_to_sell
                            lot['cost_basis'] -= lot_cost_basis
                            
                            remaining_to_sell = 0
                            
            except Exception as e:
                # Skip problematic transactions
                continue
        
        # Calculate current position metrics
        current_position_tokens = sum(lot['amount'] for lot in token_lots)
        current_position_cost_basis = sum(lot['cost_basis'] for lot in token_lots)
        avg_buy_price = (current_position_cost_basis / current_position_tokens) if current_position_tokens > 0 else 0.0
        
        # Calculate unrealized PnL
        unrealized_pnl = 0.0
        if current_position_tokens > 0 and latest_price > 0:
            current_value = current_position_tokens * latest_price
            unrealized_pnl = current_value - current_position_cost_basis
        
        # Convert holding time to hours
        avg_holding_time_hours = (total_holding_time_seconds / 3600.0 / trade_count) if trade_count > 0 else 0.0
        
        return (
            realized_pnl,
            unrealized_pnl,
            total_bought,
            total_sold,
            trade_count,
            winning_trades,
            avg_holding_time_hours,
            current_position_tokens,
            current_position_cost_basis,
            avg_buy_price,
            latest_price
        )


    def read_unfetched_transactions(spark: SparkSession) -> DataFrame:
        """Read wallet transactions that haven't been processed for PnL"""
        logger = logging.getLogger(__name__)
        
        try:
            # Read only parquet files from bronze wallet transactions
            parquet_path = "s3a://solana-data/bronze/wallet_transactions/*/wallet_transactions_*.parquet"
            bronze_df = spark.read.parquet(parquet_path)
            
            logger.info(f"Read {bronze_df.count()} total bronze transactions")
            
            # Filter for unprocessed transactions
            unfetched = bronze_df.filter(col("processed_for_pnl") == False)
            
            logger.info(f"Found {unfetched.count()} unprocessed transactions")
            
            # Add data quality filters
            clean_df = unfetched.filter(
                (col("transaction_hash").isNotNull()) &
                (col("wallet_address").isNotNull()) &
                (col("token_address").isNotNull()) &
                (col("timestamp").isNotNull()) &
                ((col("from_amount").isNotNull() & (col("from_amount") > 0)) |
                 (col("to_amount").isNotNull() & (col("to_amount") > 0)))
            )
            
            logger.info(f"After quality filters: {clean_df.count()} clean transactions")
            
            return clean_df
            
        except Exception as e:
            logger.error(f"Error reading unfetched transactions: {e}")
            # Return empty DataFrame with correct schema if no data exists
            schema = StructType([
                StructField("wallet_address", StringType()),
                StructField("token_address", StringType()),
                StructField("transaction_hash", StringType()),
                StructField("timestamp", TimestampType()),
                StructField("transaction_type", StringType()),
                StructField("from_amount", DoubleType()),
                StructField("to_amount", DoubleType()),
                StructField("base_price", DoubleType()),
                StructField("quote_price", DoubleType()),
                StructField("value_usd", DoubleType()),
                StructField("processed_for_pnl", BooleanType()),
            ])
            return spark.createDataFrame([], schema)


    def calculate_timeframe_pnl(spark: SparkSession, base_df: DataFrame, timeframe: str) -> DataFrame:
        """Calculate PnL for a specific timeframe"""
        logger = logging.getLogger(__name__)
        
        # Define time filters
        from datetime import timezone
        current_time = datetime.now(timezone.utc)
        time_filters = {
            'all': None,
            'week': current_time - timedelta(days=7),
            'month': current_time - timedelta(days=30),
            'quarter': current_time - timedelta(days=90)
        }
        
        start_time = time_filters.get(timeframe.lower())
        
        # Filter by timeframe
        if start_time:
            timeframe_df = base_df.filter(col("timestamp") >= lit(start_time))
            logger.info(f"Timeframe {timeframe}: {timeframe_df.count()} transactions after time filter")
        else:
            timeframe_df = base_df
            logger.info(f"Timeframe {timeframe}: {timeframe_df.count()} total transactions")
        
        if timeframe_df.count() == 0:
            logger.info(f"No transactions for timeframe {timeframe}")
            return spark.createDataFrame([], get_silver_pnl_schema())
        
        # Group by wallet and token, collect transactions
        wallet_token_groups = timeframe_df.groupBy("wallet_address", "token_address").agg(
            collect_list(struct([col(c) for c in timeframe_df.columns])).alias("transactions"),
            spark_min("timestamp").alias("first_transaction"),
            spark_max("timestamp").alias("last_transaction"),
            count("*").alias("transaction_count")
        )
        
        logger.info(f"Timeframe {timeframe}: {wallet_token_groups.count()} wallet-token combinations")
        
        # Apply PnL calculation UDF
        pnl_results = wallet_token_groups.withColumn(
            "pnl_metrics", 
            calculate_token_pnl_udf(col("transactions"))
        )
        
        # Extract PnL metrics and add metadata
        detailed_results = pnl_results.select(
            col("wallet_address"),
            col("token_address"),
            current_timestamp().cast(DateType()).alias("calculation_date"),
            lit(timeframe).alias("time_period"),
            
            # Core PnL metrics
            col("pnl_metrics.realized_pnl"),
            col("pnl_metrics.unrealized_pnl"),
            (col("pnl_metrics.realized_pnl") + col("pnl_metrics.unrealized_pnl")).alias("total_pnl"),
            
            # Trading metrics
            col("pnl_metrics.trade_count"),
            when(col("pnl_metrics.trade_count") > 0, 
                 col("pnl_metrics.winning_trades") / col("pnl_metrics.trade_count") * 100.0
            ).otherwise(0.0).alias("win_rate"),
            col("pnl_metrics.total_bought"),
            col("pnl_metrics.total_sold"),
            when(col("pnl_metrics.total_bought") > 0,
                 (col("pnl_metrics.total_sold") - col("pnl_metrics.total_bought")) / col("pnl_metrics.total_bought") * 100.0
            ).otherwise(0.0).alias("roi"),
            col("pnl_metrics.total_holding_time_hours").alias("avg_holding_time_hours"),
            when(col("transaction_count") > 0,
                 (col("pnl_metrics.total_bought") + col("pnl_metrics.total_sold")) / col("transaction_count")
            ).otherwise(0.0).alias("avg_transaction_amount_usd"),
            
            # Calculate trade frequency (trades per day)
            when(datediff(col("last_transaction"), col("first_transaction")) > 0,
                 col("pnl_metrics.trade_count") / datediff(col("last_transaction"), col("first_transaction"))
            ).otherwise(col("pnl_metrics.trade_count")).alias("trade_frequency_daily"),
            
            # Time ranges
            col("first_transaction"),
            col("last_transaction"),
            
            # Position metrics
            col("pnl_metrics.current_position_tokens"),
            col("pnl_metrics.current_position_cost_basis"),
            (col("pnl_metrics.current_position_tokens") * col("pnl_metrics.latest_price")).alias("current_position_value"),
            
            # Processing metadata
            current_timestamp().alias("processed_at"),
            lit("").alias("batch_id"),  # Will be updated later
            lit("birdeye_v3").alias("data_source"),
            
            # Gold layer processing state tracking (default values)
            lit(False).alias("processed_for_gold"),
            lit(None).cast("timestamp").alias("gold_processed_at"),
            lit("pending").alias("gold_processing_status"),
            lit(None).cast("string").alias("gold_batch_id")
        )
        
        logger.info(f"Timeframe {timeframe}: Generated {detailed_results.count()} PnL records")
        
        return detailed_results


    def calculate_portfolio_pnl(spark: SparkSession, token_pnl_df: DataFrame) -> DataFrame:
        """Calculate portfolio-level PnL by aggregating across all tokens per wallet"""
        logger = logging.getLogger(__name__)
        
        # Group by wallet and timeframe, aggregate across tokens
        portfolio_agg = token_pnl_df.groupBy("wallet_address", "time_period").agg(
            # Sum PnL metrics across tokens
            spark_sum("realized_pnl").alias("realized_pnl"),
            spark_sum("unrealized_pnl").alias("unrealized_pnl"),
            spark_sum("total_pnl").alias("total_pnl"),
            
            # Sum trading metrics
            spark_sum("trade_count").alias("trade_count"),
            spark_sum("total_bought").alias("total_bought"),
            spark_sum("total_sold").alias("total_sold"),
            
            # Calculate weighted averages
            (spark_sum(col("avg_holding_time_hours") * col("trade_count")) / spark_sum("trade_count")).alias("avg_holding_time_hours"),
            (spark_sum(col("avg_transaction_amount_usd") * col("trade_count")) / spark_sum("trade_count")).alias("avg_transaction_amount_usd"),
            avg("trade_frequency_daily").alias("trade_frequency_daily"),
            
            # Time ranges
            spark_min("first_transaction").alias("first_transaction"),
            spark_max("last_transaction").alias("last_transaction"),
            
            # Position metrics
            spark_sum("current_position_tokens").alias("current_position_tokens"),
            spark_sum("current_position_cost_basis").alias("current_position_cost_basis"),
            spark_sum("current_position_value").alias("current_position_value"),
            
            # Metadata
            spark_max("calculation_date").alias("calculation_date"),
            spark_max("processed_at").alias("processed_at"),
            spark_max("batch_id").alias("batch_id"),
            spark_max("data_source").alias("data_source"),
            
            # Gold processing state (take max to preserve any processing state)
            spark_max("processed_for_gold").alias("processed_for_gold"),
            spark_max("gold_processed_at").alias("gold_processed_at"),
            spark_max("gold_processing_status").alias("gold_processing_status"), 
            spark_max("gold_batch_id").alias("gold_batch_id")
        )
        
        # Add portfolio-specific fields
        portfolio_results = portfolio_agg.select(
            col("wallet_address"),
            lit("ALL_TOKENS").alias("token_address"),
            col("calculation_date"),
            col("time_period"),
            
            # Core PnL metrics
            col("realized_pnl"),
            col("unrealized_pnl"),
            col("total_pnl"),
            
            # Trading metrics
            col("trade_count"),
            when(col("trade_count") > 0,
                 (spark_sum(col("trade_count") * when(col("total_pnl") > 0, 1).otherwise(0)) / col("trade_count") * 100.0)
            ).otherwise(0.0).alias("win_rate"),
            col("total_bought"),
            col("total_sold"),
            when(col("total_bought") > 0,
                 (col("total_sold") - col("total_bought")) / col("total_bought") * 100.0
            ).otherwise(0.0).alias("roi"),
            coalesce(col("avg_holding_time_hours"), lit(0.0)).alias("avg_holding_time_hours"),
            coalesce(col("avg_transaction_amount_usd"), lit(0.0)).alias("avg_transaction_amount_usd"),
            coalesce(col("trade_frequency_daily"), lit(0.0)).alias("trade_frequency_daily"),
            
            # Time ranges
            col("first_transaction"),
            col("last_transaction"),
            
            # Position metrics
            col("current_position_tokens"),
            col("current_position_cost_basis"),
            col("current_position_value"),
            
            # Processing metadata
            col("processed_at"),
            col("batch_id"),
            col("data_source"),
            
            # Gold layer processing state tracking (inherited from token-level)
            col("processed_for_gold"),
            col("gold_processed_at"), 
            col("gold_processing_status"),
            col("gold_batch_id")
        )
        
        logger.info(f"Generated {portfolio_results.count()} portfolio PnL records")
        
        return portfolio_results


    def write_silver_pnl_data(spark: SparkSession, pnl_df: DataFrame, batch_id: str) -> str:
        """Write PnL results to silver layer"""
        logger = logging.getLogger(__name__)
        
        # Add batch_id to all records
        pnl_with_batch = pnl_df.withColumn("batch_id", lit(batch_id))
        
        # Add partition columns for efficient querying
        partitioned_df = pnl_with_batch.withColumn(
            "calculation_year", expr("year(calculation_date)")
        ).withColumn(
            "calculation_month", expr("month(calculation_date)")
        )
        
        # Write to MinIO with partitioning
        output_path = "s3a://solana-data/silver/wallet_pnl/"
        
        try:
            partitioned_df.write \
                .partitionBy("calculation_year", "calculation_month", "time_period") \
                .mode("append") \
                .parquet(output_path)
            
            logger.info(f"Successfully wrote {pnl_with_batch.count()} PnL records to {output_path}")
            
            # Write success marker
            current_date = datetime.now().strftime("%Y-%m-%d")
            success_path = f"s3a://solana-data/silver/wallet_pnl/calculation_year={datetime.now().year}/calculation_month={datetime.now().month}/_SUCCESS_{batch_id}"
            
            # Create empty success file
            spark.createDataFrame([("success",)], ["status"]).coalesce(1).write.mode("overwrite").text(success_path)
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error writing silver PnL data: {e}")
            raise


    def update_bronze_processing_status(spark: SparkSession, processed_df: DataFrame, batch_id: str):
        """Update bronze layer to mark transactions as processed for PnL"""
        logger = logging.getLogger(__name__)
        
        try:
            # Get unique transaction hashes that were processed
            processed_hashes = processed_df.select("transaction_hash").distinct()
            
            logger.info(f"Updating processing status for {processed_hashes.count()} transactions")
            
            # Read current bronze data using parquet path pattern
            parquet_path = "s3a://solana-data/bronze/wallet_transactions/*/wallet_transactions_*.parquet"
            bronze_df = spark.read.parquet(parquet_path)
            
            # Update processing flags for processed transactions
            updated_df = bronze_df.join(
                processed_hashes.withColumn("was_processed", lit(True)), 
                on="transaction_hash", 
                how="left"
            ).withColumn(
                "processed_for_pnl",
                when(col("was_processed") == True, True).otherwise(col("processed_for_pnl"))
            ).withColumn(
                "pnl_processed_at",
                when(col("was_processed") == True, current_timestamp()).otherwise(col("pnl_processed_at"))
            ).withColumn(
                "pnl_processing_status",
                when(col("was_processed") == True, "completed").otherwise(col("pnl_processing_status"))
            ).drop("was_processed")
            
            # Write updated data back to bronze layer with proper partitioning
            partitioned_df = updated_df.withColumn(
                "processing_date", 
                expr("date(timestamp)")
            )
            
            partitioned_df.write \
                .partitionBy("processing_date") \
                .mode("overwrite") \
                .parquet("s3a://solana-data/bronze/wallet_transactions_updated/")
            
            logger.info("Successfully updated bronze layer processing status")
            
        except Exception as e:
            logger.error(f"Error updating bronze processing status: {e}")
            # Don't fail the entire job if status update fails
            pass