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

def bronze_token_list_task(**context):
    """Task 1: Fetch token list from BirdEye API"""
    from tasks.bronze_tasks import fetch_bronze_token_list
    return fetch_bronze_token_list(**context)

def silver_tracked_tokens_task(**context):
    """Task 2: Filter high-performance tokens"""
    from tasks.silver_tasks import transform_silver_tracked_tokens
    return transform_silver_tracked_tokens(**context)

def bronze_token_whales_task(**context):
    """Task 3: Fetch whale data for tracked tokens"""
    from tasks.bronze_tasks import fetch_bronze_token_whales
    return fetch_bronze_token_whales(**context)

def bronze_wallet_transactions_task(**context):
    """Task 4: Fetch transaction history for whale wallets"""
    from tasks.bronze_tasks import fetch_bronze_wallet_transactions
    return fetch_bronze_wallet_transactions(**context)

def gold_top_traders_task(**context):
    """Task 6: Create top trader analytics"""
    from tasks.gold_tasks import transform_gold_top_traders
    return transform_gold_top_traders(**context)

def helius_webhook_update_task(**context):
    """Task 7: Push top traders to Helius (non-critical)"""
    from tasks.helius_tasks import update_helius_webhook
    return update_helius_webhook(**context)

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
    
    # Create Spark session with S3A/MinIO support
    spark = SparkSession.builder \
        .appName(f"SilverWalletPnL_{batch_id}") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Define FIFO UDF (embedded for task)
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
        """FIFO PnL calculation UDF"""
        if not transactions:
            return (0.0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)
        
        # Convert to list and sort by timestamp
        txn_list = []
        for row in transactions:
            txn_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            txn_list.append(txn_dict)
        
        txn_list.sort(key=lambda x: x['timestamp'])
        
        # FIFO calculation
        token_lots = []
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
                
                if hasattr(timestamp, 'timestamp'):
                    timestamp_unix = timestamp.timestamp()
                else:
                    timestamp_unix = float(timestamp)
                
                price = base_price or quote_price or 0
                if price > 0:
                    latest_price = price
                
                # Process BUY transactions
                if tx_type == 'BUY' and to_amount > 0:
                    if value_usd > 0:
                        cost_basis = value_usd
                        buy_price = cost_basis / to_amount
                    elif price > 0:
                        buy_price = price
                        cost_basis = to_amount * buy_price
                    else:
                        continue
                    
                    token_lots.append({
                        'amount': to_amount,
                        'price': buy_price,
                        'cost_basis': cost_basis,
                        'timestamp': timestamp_unix
                    })
                    total_bought += cost_basis
                
                # Process SELL transactions
                elif tx_type == 'SELL' and from_amount > 0:
                    if value_usd > 0:
                        sale_value = value_usd
                        sell_price = sale_value / from_amount
                    elif price > 0:
                        sell_price = price
                        sale_value = from_amount * sell_price
                    else:
                        continue
                    
                    total_sold += sale_value
                    remaining_to_sell = from_amount
                    
                    while remaining_to_sell > 0 and token_lots:
                        lot = token_lots[0]
                        
                        if lot['amount'] <= remaining_to_sell:
                            # Sell entire lot
                            lot_sale_value = lot['amount'] * sell_price
                            lot_pnl = lot_sale_value - lot['cost_basis']
                            realized_pnl += lot_pnl
                            
                            trade_count += 1
                            if lot_pnl > 0:
                                winning_trades += 1
                            
                            holding_time = timestamp_unix - lot['timestamp']
                            total_holding_time_seconds += holding_time
                            
                            remaining_to_sell -= lot['amount']
                            token_lots.pop(0)
                        else:
                            # Sell partial lot
                            sell_fraction = remaining_to_sell / lot['amount']
                            lot_cost_basis = lot['cost_basis'] * sell_fraction
                            lot_sale_value = remaining_to_sell * sell_price
                            lot_pnl = lot_sale_value - lot_cost_basis
                            realized_pnl += lot_pnl
                            
                            trade_count += 1
                            if lot_pnl > 0:
                                winning_trades += 1
                            
                            holding_time = timestamp_unix - lot['timestamp']
                            total_holding_time_seconds += holding_time
                            
                            lot['amount'] -= remaining_to_sell
                            lot['cost_basis'] -= lot_cost_basis
                            remaining_to_sell = 0
                            
            except Exception:
                continue
        
        # Calculate current position
        current_position_tokens = sum(lot['amount'] for lot in token_lots)
        current_position_cost_basis = sum(lot['cost_basis'] for lot in token_lots)
        avg_buy_price = (current_position_cost_basis / current_position_tokens) if current_position_tokens > 0 else 0.0
        
        # Calculate unrealized PnL
        unrealized_pnl = 0.0
        if current_position_tokens > 0 and latest_price > 0:
            current_value = current_position_tokens * latest_price
            unrealized_pnl = current_value - current_position_cost_basis
        
        avg_holding_time_hours = (total_holding_time_seconds / 3600.0 / trade_count) if trade_count > 0 else 0.0
        
        return (
            realized_pnl, unrealized_pnl, total_bought, total_sold,
            trade_count, winning_trades, avg_holding_time_hours,
            current_position_tokens, current_position_cost_basis,
            avg_buy_price, latest_price
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