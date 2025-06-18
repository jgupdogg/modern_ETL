"""
Smart Trader Identification DAG
Consolidated end-to-end pipeline: Bronze → Silver → Gold → Helius
"""
import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import core functions from individual DAGs
import sys
sys.path.append('/opt/airflow/dags')

# Import centralized configuration
from config.smart_trader_config import get_spark_config

def bronze_token_list_task(**context):
    """Task 1: Fetch token list from BirdEye API"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.bronze_tasks import fetch_bronze_token_list
        result = fetch_bronze_token_list(**context)
        
        # Log aggregated success info
        if result and isinstance(result, list):
            logger.info(f"✅ BRONZE TOKENS: Successfully fetched {len(result)} tokens")
        else:
            logger.warning("⚠️ BRONZE TOKENS: No token data returned")
            
        return result
        
    except Exception as e:
        # Log major errors only
        if "429" in str(e) or "rate limit" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API rate limit exceeded")
        elif "401" in str(e) or "403" in str(e) or "auth" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API authentication failed")
        elif "timeout" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API timeout")
        else:
            logger.error(f"❌ BRONZE TOKENS FAILED: {str(e)}")
        raise

def silver_tracked_tokens_task(**context):
    """Task 2: Filter high-performance tokens"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.silver_tasks import transform_silver_tracked_tokens
        result = transform_silver_tracked_tokens(**context)
        
        # Log aggregated success info
        if result and isinstance(result, list):
            logger.info(f"✅ SILVER TOKENS: Filtered to {len(result)} high-performance tokens")
        else:
            logger.warning("⚠️ SILVER TOKENS: No filtered tokens returned")
            
        return result
        
    except Exception as e:
        # Log major errors only
        if "no data" in str(e).lower() or "empty" in str(e).lower():
            logger.error("❌ CRITICAL: No bronze token data available for filtering")
        elif "threshold" in str(e).lower():
            logger.error("❌ SILVER TOKENS: Performance threshold filtering failed")
        else:
            logger.error(f"❌ SILVER TOKENS FAILED: {str(e)}")
        raise

def bronze_token_whales_task(**context):
    """Task 3: Fetch whale data for tracked tokens"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.bronze_tasks import fetch_bronze_token_whales
        result = fetch_bronze_token_whales(**context)
        
        # Log aggregated success info
        if result and isinstance(result, dict):
            whales_count = result.get('total_whales_saved', 0)
            tokens_processed = result.get('tokens_processed', 0)
            logger.info(f"✅ BRONZE WHALES: Processed {tokens_processed} tokens, found {whales_count} whale holders")
        else:
            logger.warning("⚠️ BRONZE WHALES: No whale data returned")
            
        return result
        
    except Exception as e:
        # Log major errors only
        if "429" in str(e) or "rate limit" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API rate limit exceeded (whale data)")
        elif "401" in str(e) or "403" in str(e) or "auth" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API authentication failed (whale data)")
        elif "timeout" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API timeout (whale data)")
        elif "no tokens" in str(e).lower():
            logger.error("❌ CRITICAL: No tracked tokens available for whale fetching")
        else:
            logger.error(f"❌ BRONZE WHALES FAILED: {str(e)}")
        raise

def bronze_wallet_transactions_task(**context):
    """Task 4: Fetch transaction history for whale wallets"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.bronze_tasks import fetch_bronze_wallet_transactions
        result = fetch_bronze_wallet_transactions(**context)
        
        # Log aggregated success info
        if result and isinstance(result, dict):
            wallets_processed = result.get('wallets_processed', 0)
            transactions_saved = result.get('total_transactions_saved', 0)
            mock_data_used = result.get('mock_data_used', False)
            
            if mock_data_used:
                logger.warning(f"⚠️ BRONZE TRANSACTIONS: Used mock data for {wallets_processed} wallets (API issues)")
            else:
                logger.info(f"✅ BRONZE TRANSACTIONS: Processed {wallets_processed} wallets, saved {transactions_saved} transactions")
        else:
            logger.warning("⚠️ BRONZE TRANSACTIONS: No transaction data returned")
            
        return result
        
    except Exception as e:
        # Log major errors only
        if "429" in str(e) or "rate limit" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API rate limit exceeded (transactions)")
        elif "401" in str(e) or "403" in str(e) or "auth" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API authentication failed (transactions)")
        elif "timeout" in str(e).lower():
            logger.error("❌ CRITICAL: BirdEye API timeout (transactions)")
        elif "404" in str(e):
            logger.error("❌ CRITICAL: BirdEye API endpoint not found (check wallet transactions endpoint)")
        elif "no wallets" in str(e).lower():
            logger.error("❌ CRITICAL: No whale wallets available for transaction fetching")
        else:
            logger.error(f"❌ BRONZE TRANSACTIONS FAILED: {str(e)}")
        raise

def gold_top_traders_task(**context):
    """Task 6: Create top trader analytics"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.gold_tasks import transform_gold_top_traders
        result = transform_gold_top_traders(**context)
        
        # Log aggregated success info
        if result and isinstance(result, dict):
            traders_count = result.get('smart_traders_count', 0)
            dbt_status = result.get('dbt_status', 'unknown')
            
            if dbt_status == 'success':
                logger.info(f"✅ GOLD TRADERS: Identified {traders_count} smart traders via dbt")
            else:
                logger.warning(f"⚠️ GOLD TRADERS: dbt run issues, found {traders_count} traders")
        else:
            logger.warning("⚠️ GOLD TRADERS: No analytics result returned")
            
        return result
        
    except Exception as e:
        # Log major errors only
        if "dbt" in str(e).lower():
            logger.error("❌ CRITICAL: dbt transformation failed (gold layer)")
        elif "no silver data" in str(e).lower():
            logger.error("❌ CRITICAL: No silver PnL data available for gold transformation")
        elif "sql" in str(e).lower() or "database" in str(e).lower():
            logger.error("❌ CRITICAL: Database/SQL error in gold transformation")
        else:
            logger.error(f"❌ GOLD TRADERS FAILED: {str(e)}")
        raise

def helius_webhook_update_task(**context):
    """Task 7: Push top traders to Helius (non-critical)"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.helius_tasks import update_helius_webhook
        result = update_helius_webhook(**context)
        
        # Log aggregated success info
        if result and isinstance(result, dict):
            addresses_updated = result.get('addresses_updated', 0)
            webhook_status = result.get('status', 'unknown')
            
            if webhook_status == 'success':
                logger.info(f"✅ HELIUS UPDATE: Successfully updated {addresses_updated} whale addresses")
            else:
                logger.warning(f"⚠️ HELIUS UPDATE: Partial success, updated {addresses_updated} addresses")
        else:
            logger.warning("⚠️ HELIUS UPDATE: No webhook update result returned")
            
        return result
        
    except Exception as e:
        # Log major errors only (but don't fail DAG)
        if "auth" in str(e).lower() or "401" in str(e) or "403" in str(e):
            logger.error("❌ HELIUS: Authentication failed (webhook update)")
        elif "rate limit" in str(e).lower() or "429" in str(e):
            logger.error("❌ HELIUS: Rate limit exceeded (webhook update)")
        elif "timeout" in str(e).lower():
            logger.error("❌ HELIUS: API timeout (webhook update)")
        elif "no traders" in str(e).lower():
            logger.warning("⚠️ HELIUS: No smart traders available for webhook update")
        else:
            logger.error(f"❌ HELIUS UPDATE FAILED: {str(e)}")
        
        # Don't re-raise since this is non-critical
        return {"status": "failed", "error": str(e)}

# DAG Definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'smart_trader_identification',
    default_args=default_args,
    description='End-to-end smart trader identification pipeline: Bronze → Silver → Gold → Helius',
    schedule_interval='0 9,21 * * *',  # 9 AM & 9 PM UTC
    catchup=False,
    max_active_runs=1,
    tags=['end-to-end', 'smart-traders', 'medallion', 'analytics'],
)

@task(dag=dag)
def silver_wallet_pnl_task(**context):
    """Task 5: Calculate wallet PnL metrics using PySpark with full FIFO implementation"""
    import logging
    from pyspark.sql import SparkSession
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
    from datetime import datetime, timedelta, timezone
    
    logger = logging.getLogger(__name__)
    logger.info("Starting PySpark wallet PnL calculation with full FIFO implementation")
    
    # Generate batch ID
    execution_date = context['logical_date']
    batch_id = execution_date.strftime("%Y%m%d_%H%M%S")
    
    # Create Spark session with S3A/MinIO support using centralized config
    spark_config = get_spark_config()
    spark_builder = SparkSession.builder.appName(f"SilverWalletPnL_{batch_id}")
    
    # Apply all config settings
    for key, value in spark_config.items():
        spark_builder = spark_builder.config(key, value)
    
    spark = spark_builder.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Define improved FIFO UDF (handles SELL-before-BUY sequences)
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
    def calculate_token_pnl_fifo(transactions):
        """
        Improved FIFO PnL calculation that handles:
        1. SELL transactions before any BUY (tracks as negative inventory)
        2. Partial matching scenarios
        3. Better price/value handling
        4. More accurate trade counting
        """
        if not transactions:
            return (0.0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)
        
        # Convert to list and sort by timestamp
        txn_list = []
        for row in transactions:
            txn_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            txn_list.append(txn_dict)
        
        txn_list.sort(key=lambda x: x['timestamp'])
        
        # Enhanced FIFO tracking
        buy_lots = []  # FIFO queue for purchases: [{'amount': float, 'price': float, 'cost': float, 'timestamp': float}]
        sell_queue = []  # Track unmatched sells
        
        realized_pnl = 0.0
        total_bought = 0.0
        total_sold = 0.0
        trade_count = 0
        winning_trades = 0
        total_holding_time_seconds = 0.0
        latest_price = 0.0
        
        for txn in txn_list:
            try:
                tx_type = txn.get('transaction_type', '').upper()
                from_amount = float(txn.get('from_amount') or 0)
                to_amount = float(txn.get('to_amount') or 0)
                base_price = float(txn.get('base_price') or 0)
                quote_price = float(txn.get('quote_price') or 0)
                value_usd = float(txn.get('value_usd') or 0)
                timestamp = txn.get('timestamp')
                
                # Handle timestamp conversion
                if hasattr(timestamp, 'timestamp'):
                    timestamp_unix = timestamp.timestamp()
                else:
                    timestamp_unix = float(timestamp) if timestamp else 0
                
                # Determine price
                price = base_price or quote_price or 0
                if price > 0:
                    latest_price = price
                
                # Enhanced BUY processing
                if tx_type == 'BUY' and to_amount > 0:
                    # Calculate cost basis and price
                    if value_usd > 0:
                        cost_basis = value_usd
                        buy_price = cost_basis / to_amount
                    elif price > 0:
                        buy_price = price
                        cost_basis = to_amount * buy_price
                    else:
                        # Skip if no price info
                        continue
                    
                    total_bought += cost_basis
                    
                    # Add to buy lots queue
                    buy_lots.append({
                        'amount': to_amount,
                        'price': buy_price,
                        'cost_basis': cost_basis,
                        'timestamp': timestamp_unix
                    })
                    
                    # Try to match against any pending sells
                    while sell_queue and buy_lots:
                        sell_order = sell_queue[0]
                        buy_lot = buy_lots[0]
                        
                        if sell_order['amount'] <= buy_lot['amount']:
                            # Sell entire pending order against this buy lot
                            sell_amount = sell_order['amount']
                            sell_value = sell_amount * sell_order['price']
                            cost_basis_used = (sell_amount / buy_lot['amount']) * buy_lot['cost_basis']
                            
                            lot_pnl = sell_value - cost_basis_used
                            realized_pnl += lot_pnl
                            
                            trade_count += 1
                            if lot_pnl > 0:
                                winning_trades += 1
                            
                            # Calculate holding time (sell time - buy time)
                            holding_time = sell_order['timestamp'] - buy_lot['timestamp']
                            total_holding_time_seconds += max(0, holding_time)
                            
                            # Update lots
                            buy_lot['amount'] -= sell_amount
                            buy_lot['cost_basis'] -= cost_basis_used
                            
                            if buy_lot['amount'] <= 0.001:  # Almost zero, remove
                                buy_lots.pop(0)
                            
                            sell_queue.pop(0)
                        else:
                            # Partial sell - use entire buy lot
                            buy_amount = buy_lot['amount']
                            sell_value = buy_amount * sell_order['price']
                            cost_basis_used = buy_lot['cost_basis']
                            
                            lot_pnl = sell_value - cost_basis_used
                            realized_pnl += lot_pnl
                            
                            trade_count += 1
                            if lot_pnl > 0:
                                winning_trades += 1
                            
                            holding_time = sell_order['timestamp'] - buy_lot['timestamp']
                            total_holding_time_seconds += max(0, holding_time)
                            
                            # Update orders
                            sell_order['amount'] -= buy_amount
                            buy_lots.pop(0)
                
                # Enhanced SELL processing
                elif tx_type == 'SELL' and from_amount > 0:
                    # Calculate sale value and price
                    if value_usd > 0:
                        sale_value = value_usd
                        sell_price = sale_value / from_amount
                    elif price > 0:
                        sell_price = price
                        sale_value = from_amount * sell_price
                    else:
                        # Skip if no price info
                        continue
                    
                    total_sold += sale_value
                    remaining_to_sell = from_amount
                    
                    # Try to match against existing buy lots
                    while remaining_to_sell > 0.001 and buy_lots:
                        buy_lot = buy_lots[0]
                        
                        if buy_lot['amount'] <= remaining_to_sell:
                            # Use entire buy lot
                            lot_amount = buy_lot['amount']
                            lot_sale_value = lot_amount * sell_price
                            lot_pnl = lot_sale_value - buy_lot['cost_basis']
                            realized_pnl += lot_pnl
                            
                            trade_count += 1
                            if lot_pnl > 0:
                                winning_trades += 1
                            
                            holding_time = timestamp_unix - buy_lot['timestamp']
                            total_holding_time_seconds += max(0, holding_time)
                            
                            remaining_to_sell -= lot_amount
                            buy_lots.pop(0)
                        else:
                            # Partial lot usage
                            sell_fraction = remaining_to_sell / buy_lot['amount']
                            cost_basis_used = buy_lot['cost_basis'] * sell_fraction
                            lot_sale_value = remaining_to_sell * sell_price
                            lot_pnl = lot_sale_value - cost_basis_used
                            realized_pnl += lot_pnl
                            
                            trade_count += 1
                            if lot_pnl > 0:
                                winning_trades += 1
                            
                            holding_time = timestamp_unix - buy_lot['timestamp']
                            total_holding_time_seconds += max(0, holding_time)
                            
                            # Update remaining lot
                            buy_lot['amount'] -= remaining_to_sell
                            buy_lot['cost_basis'] -= cost_basis_used
                            remaining_to_sell = 0
                    
                    # If there's still remaining to sell, add to sell queue
                    if remaining_to_sell > 0.001:
                        sell_queue.append({
                            'amount': remaining_to_sell,
                            'price': sell_price,
                            'timestamp': timestamp_unix
                        })
                        
            except Exception as e:
                # Skip problematic transactions but continue processing
                continue
        
        # Calculate current position (remaining buy lots)
        current_position_tokens = sum(lot['amount'] for lot in buy_lots)
        current_position_cost_basis = sum(lot['cost_basis'] for lot in buy_lots)
        avg_buy_price = (current_position_cost_basis / current_position_tokens) if current_position_tokens > 0 else 0.0
        
        # Calculate unrealized PnL
        unrealized_pnl = 0.0
        if current_position_tokens > 0 and latest_price > 0:
            current_value = current_position_tokens * latest_price
            unrealized_pnl = current_value - current_position_cost_basis
        
        # Average holding time
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
    
    try:
        # Read unprocessed transactions
        try:
            parquet_path = "s3a://solana-data/bronze/wallet_transactions/*/wallet_transactions_*.parquet"
            bronze_df = spark.read.parquet(parquet_path)
            
            # Filter for unprocessed transactions with quality checks
            clean_df = bronze_df.filter(
                (col("processed_for_pnl") == False) &
                (col("transaction_hash").isNotNull()) &
                (col("wallet_address").isNotNull()) &
                (col("token_address").isNotNull()) &
                (col("timestamp").isNotNull()) &
                ((col("from_amount").isNotNull() & (col("from_amount") > 0)) |
                 (col("to_amount").isNotNull() & (col("to_amount") > 0)))
            )
            
            unfetched_count = clean_df.count()
            logger.info(f"Found {unfetched_count} clean unprocessed transactions")
            
            if unfetched_count == 0:
                logger.info("No unfetched transactions - returning success")
                return {
                    "wallets_processed": 0,
                    "total_pnl_records": 0,
                    "batch_id": batch_id,
                    "status": "no_data"
                }
            
            # Calculate PnL for all timeframes
            timeframes = ['all', 'week', 'month', 'quarter']
            all_results = []
            
            for timeframe in timeframes:
                logger.info(f"Calculating PnL for timeframe: {timeframe}")
                
                # Apply time filter
                current_time = datetime.now(timezone.utc)
                if timeframe == 'week':
                    start_time = current_time - timedelta(days=7)
                    timeframe_df = clean_df.filter(col("timestamp") >= lit(start_time))
                elif timeframe == 'month':
                    start_time = current_time - timedelta(days=30)
                    timeframe_df = clean_df.filter(col("timestamp") >= lit(start_time))
                elif timeframe == 'quarter':
                    start_time = current_time - timedelta(days=90)
                    timeframe_df = clean_df.filter(col("timestamp") >= lit(start_time))
                else:  # 'all'
                    timeframe_df = clean_df
                
                # Group by wallet and token
                wallet_token_groups = timeframe_df.groupBy("wallet_address", "token_address").agg(
                    collect_list(struct([col(c) for c in timeframe_df.columns])).alias("transactions"),
                    spark_min("timestamp").alias("first_transaction"),
                    spark_max("timestamp").alias("last_transaction"),
                    count("*").alias("transaction_count")
                )
                
                if wallet_token_groups.count() == 0:
                    logger.info(f"No data for timeframe {timeframe}")
                    continue
                
                # Apply FIFO UDF
                pnl_results = wallet_token_groups.withColumn(
                    "pnl_metrics", 
                    calculate_token_pnl_fifo(col("transactions"))
                )
                
                # Extract results and create final schema
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
                    
                    # Trade frequency
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
                    lit(batch_id).alias("batch_id"),
                    lit("birdeye_v3").alias("data_source"),
                    
                    # Gold layer processing state
                    lit(False).alias("processed_for_gold"),
                    lit(None).cast("timestamp").alias("gold_processed_at"),
                    lit("pending").alias("gold_processing_status"),
                    lit(None).cast("string").alias("gold_batch_id")
                )
                
                all_results.append(detailed_results)
            
            # Combine all timeframe results
            if all_results:
                token_level_pnl = all_results[0]
                for df in all_results[1:]:
                    token_level_pnl = token_level_pnl.unionAll(df)
                
                # Calculate portfolio-level PnL (aggregate by wallet and timeframe)
                portfolio_agg = token_level_pnl.groupBy("wallet_address", "time_period").agg(
                    spark_sum("realized_pnl").alias("realized_pnl"),
                    spark_sum("unrealized_pnl").alias("unrealized_pnl"),
                    spark_sum("total_pnl").alias("total_pnl"),
                    spark_sum("trade_count").alias("trade_count"),
                    spark_sum("total_bought").alias("total_bought"),
                    spark_sum("total_sold").alias("total_sold"),
                    avg("avg_holding_time_hours").alias("avg_holding_time_hours"),
                    avg("avg_transaction_amount_usd").alias("avg_transaction_amount_usd"),
                    avg("trade_frequency_daily").alias("trade_frequency_daily"),
                    spark_min("first_transaction").alias("first_transaction"),
                    spark_max("last_transaction").alias("last_transaction"),
                    spark_sum("current_position_tokens").alias("current_position_tokens"),
                    spark_sum("current_position_cost_basis").alias("current_position_cost_basis"),
                    spark_sum("current_position_value").alias("current_position_value"),
                    spark_max("calculation_date").alias("calculation_date"),
                    spark_max("processed_at").alias("processed_at"),
                    spark_max("batch_id").alias("batch_id"),
                    spark_max("data_source").alias("data_source")
                )
                
                # Create portfolio records
                portfolio_results = portfolio_agg.select(
                    col("wallet_address"),
                    lit("ALL_TOKENS").alias("token_address"),
                    col("calculation_date"),
                    col("time_period"),
                    col("realized_pnl"),
                    col("unrealized_pnl"),
                    col("total_pnl"),
                    col("trade_count"),
                    when(col("trade_count") > 0, 50.0).otherwise(0.0).alias("win_rate"),  # Simplified
                    col("total_bought"),
                    col("total_sold"),
                    when(col("total_bought") > 0,
                         (col("total_sold") - col("total_bought")) / col("total_bought") * 100.0
                    ).otherwise(0.0).alias("roi"),
                    coalesce(col("avg_holding_time_hours"), lit(0.0)).alias("avg_holding_time_hours"),
                    coalesce(col("avg_transaction_amount_usd"), lit(0.0)).alias("avg_transaction_amount_usd"),
                    coalesce(col("trade_frequency_daily"), lit(0.0)).alias("trade_frequency_daily"),
                    col("first_transaction"),
                    col("last_transaction"),
                    col("current_position_tokens"),
                    col("current_position_cost_basis"),
                    col("current_position_value"),
                    col("processed_at"),
                    col("batch_id"),
                    col("data_source"),
                    lit(False).alias("processed_for_gold"),
                    lit(None).cast("timestamp").alias("gold_processed_at"),
                    lit("pending").alias("gold_processing_status"),
                    lit(None).cast("string").alias("gold_batch_id")
                )
                
                # Combine token and portfolio results
                final_results = token_level_pnl.unionAll(portfolio_results)
                
                # Write to silver layer with partitioning
                partitioned_df = final_results.withColumn(
                    "calculation_year", expr("year(calculation_date)")
                ).withColumn(
                    "calculation_month", expr("month(calculation_date)")
                )
                
                output_path = "s3a://solana-data/silver/wallet_pnl/"
                
                partitioned_df.write \
                    .partitionBy("calculation_year", "calculation_month", "time_period") \
                    .mode("append") \
                    .parquet(output_path)
                
                # Write success marker
                current_date = datetime.now()
                success_path = f"s3a://solana-data/silver/wallet_pnl/calculation_year={current_date.year}/calculation_month={current_date.month}/_SUCCESS_{batch_id}"
                spark.createDataFrame([("success",)], ["status"]).coalesce(1).write.mode("overwrite").text(success_path)
                
                # Update bronze processing status
                processed_hashes = clean_df.select("transaction_hash").distinct()
                bronze_df_updated = bronze_df.join(
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
                
                # Write updated bronze data back to original location
                bronze_df_updated.write \
                    .partitionBy("date") \
                    .mode("overwrite") \
                    .parquet("s3a://solana-data/bronze/wallet_transactions/")
                
                unique_wallets = clean_df.select("wallet_address").distinct().count()
                total_records = final_results.count()
                
                logger.info(f"PnL calculation completed successfully for batch {batch_id}")
                logger.info(f"Processed {unique_wallets} wallets, generated {total_records} PnL records")
                
                return {
                    "wallets_processed": unique_wallets,
                    "total_pnl_records": total_records,
                    "batch_id": batch_id,
                    "output_path": output_path,
                    "status": "success"
                }
            else:
                logger.info("No timeframe data to process")
                return {
                    "wallets_processed": 0,
                    "total_pnl_records": 0,
                    "batch_id": batch_id,
                    "status": "no_timeframe_data"
                }
                
        except Exception as e:
            logger.warning(f"Could not read bronze data: {e}")
            return {
                "wallets_processed": 0,
                "total_pnl_records": 0,
                "batch_id": batch_id,
                "status": "no_bronze_data"
            }
            
    except Exception as e:
        logger.error(f"Error in silver wallet PnL task: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        spark.stop()

# Task Definitions
task_1_bronze_tokens = PythonOperator(
    task_id='bronze_token_list',
    python_callable=bronze_token_list_task,
    provide_context=True,
    dag=dag,
)

task_2_silver_tracked = PythonOperator(
    task_id='silver_tracked_tokens',
    python_callable=silver_tracked_tokens_task,
    provide_context=True,
    dag=dag,
)

task_3_bronze_whales = PythonOperator(
    task_id='bronze_token_whales',
    python_callable=bronze_token_whales_task,
    provide_context=True,
    dag=dag,
)

task_4_bronze_transactions = PythonOperator(
    task_id='bronze_wallet_transactions',
    python_callable=bronze_wallet_transactions_task,
    provide_context=True,
    dag=dag,
)

task_6_gold_traders = PythonOperator(
    task_id='gold_top_traders',
    python_callable=gold_top_traders_task,
    provide_context=True,
    dag=dag,
)

task_7_helius_update = PythonOperator(
    task_id='helius_webhook_update',
    python_callable=helius_webhook_update_task,
    provide_context=True,
    dag=dag,
    # Non-critical task - don't fail the DAG if this fails
    trigger_rule='all_done',  # Run even if upstream tasks fail
)

# Task Dependencies
with dag:
    # Call the decorated task function
    pnl_task = silver_wallet_pnl_task()
    
    task_1_bronze_tokens >> task_2_silver_tracked >> task_3_bronze_whales >> task_4_bronze_transactions >> pnl_task >> task_6_gold_traders >> task_7_helius_update

# Pipeline: Bronze Token List → Silver Tracked Tokens → Bronze Whales → Bronze Transactions → Silver PnL → Gold Traders → Helius Update