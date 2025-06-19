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
from config.smart_trader_config import (
    get_spark_config,
    SILVER_PNL_RECENT_DAYS, SILVER_PNL_HISTORICAL_LIMIT, 
    SILVER_PNL_MIN_TRANSACTIONS, PNL_AMOUNT_PRECISION_THRESHOLD,
    PNL_BATCH_PROGRESS_INTERVAL, BRONZE_WALLET_TRANSACTIONS_PATH
)

def bronze_token_list_task(**context):
    """Task 1: Fetch token list from BirdEye API"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.bronze_tasks import fetch_bronze_token_list
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
        from tasks.smart_traders.silver_tasks import transform_silver_tracked_tokens
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
        from tasks.smart_traders.bronze_tasks import fetch_bronze_token_whales
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
        from tasks.smart_traders.bronze_tasks import fetch_bronze_wallet_transactions
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
    """Task 6: Create top trader analytics using dbt"""
    logger = logging.getLogger(__name__)
    
    try:
        import subprocess
        import json
        from datetime import datetime
        
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting dbt smart wallets transformation - batch {batch_id}")
        
        # Run dbt model for smart wallets
        env = os.environ.copy()
        env['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
        
        # Run dbt model
        result = subprocess.run(
            ['dbt', 'run', '--models', 'smart_wallets'],
            cwd='/opt/airflow/dbt',
            env=env,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"dbt run failed: {result.stderr}")
            return {
                'status': 'failed',
                'error': result.stderr,
                'batch_id': batch_id,
                'smart_traders_count': 0,
                'dbt_status': 'failed'
            }
        
        logger.info("dbt smart wallets model executed successfully")
        
        # Query results to get trader count
        validation_cmd = """
        docker exec claude_pipeline-duckdb python3 -c "
import duckdb
conn = duckdb.connect('/data/analytics.duckdb')
conn.execute('LOAD httpfs;')
conn.execute('SET s3_endpoint=\\'minio:9000\\';')
conn.execute('SET s3_access_key_id=\\'minioadmin\\';')
conn.execute('SET s3_secret_access_key=\\'minioadmin123\\';')
conn.execute('SET s3_use_ssl=false;')
conn.execute('SET s3_url_style=\\'path\\';')

try:
    # Get smart wallet counts
    smart_wallets_count = conn.execute('SELECT COUNT(DISTINCT wallet_address) FROM smart_wallets;').fetchone()[0]
    
    # Get performance tier breakdown
    tier_counts = conn.execute('SELECT performance_tier, COUNT(*) as count FROM smart_wallets GROUP BY performance_tier;').fetchall()
    tier_dict = {row[0]: row[1] for row in tier_counts}
    
    print(f'RESULT:{{\\"count\\":{smart_wallets_count},\\"tiers\\":{tier_dict}}}')
except Exception as e:
    print(f'ERROR:{e}')
conn.close()
"
        """
        
        validation_result = subprocess.run(validation_cmd, shell=True, capture_output=True, text=True)
        
        # Parse validation results
        smart_traders_count = 0
        performance_tiers = {}
        
        if "RESULT:" in validation_result.stdout:
            try:
                result_json = validation_result.stdout.split("RESULT:")[1].strip()
                result_data = json.loads(result_json)
                smart_traders_count = result_data.get('count', 0)
                performance_tiers = result_data.get('tiers', {})
            except:
                logger.warning("Could not parse validation results")
        
        logger.info(f"✅ GOLD TRADERS: Identified {smart_traders_count} smart traders via dbt")
        if performance_tiers:
            logger.info(f"Performance tier breakdown: {performance_tiers}")
        
        return {
            'status': 'success',
            'smart_traders_count': smart_traders_count,
            'performance_tiers': performance_tiers,
            'batch_id': batch_id,
            'dbt_status': 'success',
            'dbt_output': result.stdout
        }
        
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
    """Task 5: Calculate wallet PnL metrics using PySpark with raw bronze data and portfolio-only output"""
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
        collect_list, struct, udf, current_timestamp, expr, coalesce, abs as spark_abs,
        unix_timestamp, from_unixtime, datediff, hour, desc, asc, row_number, size
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, 
        TimestampType, BooleanType, ArrayType, MapType, DateType
    )
    from pyspark.sql.window import Window
    from datetime import datetime, timedelta, timezone
    
    logger = logging.getLogger(__name__)
    logger.info("Starting PySpark wallet PnL calculation with raw bronze data transformation")
    
    # Generate batch ID
    execution_date = context['logical_date']
    batch_id = execution_date.strftime("%Y%m%d_%H%M%S")
    
    # Configuration for transaction filtering (from centralized config)
    RECENT_DAYS = SILVER_PNL_RECENT_DAYS  # Include all transactions from last N days
    HISTORICAL_LIMIT = SILVER_PNL_HISTORICAL_LIMIT  # Maximum total transactions per wallet
    MIN_TRANSACTIONS = SILVER_PNL_MIN_TRANSACTIONS  # Skip wallets with too few trades
    
    # Create Spark session with S3A/MinIO support using centralized config
    spark_config = get_spark_config()
    spark_builder = SparkSession.builder.appName(f"SilverWalletPnL_{batch_id}")
    
    # Apply all config settings
    for key, value in spark_config.items():
        spark_builder = spark_builder.config(key, value)
    
    spark = spark_builder.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Define raw-to-normalized transformation UDF
    @udf(returnType=ArrayType(StructType([
        StructField("token_address", StringType()),
        StructField("transaction_type", StringType()),
        StructField("amount", DoubleType()),
        StructField("price", DoubleType()),
        StructField("value_usd", DoubleType()),
        StructField("timestamp", TimestampType()),
        StructField("transaction_hash", StringType())
    ])))
    def transform_raw_to_normalized(raw_transactions):
        """
        Transform raw bronze transactions to normalized format.
        Each swap generates TWO records: one SELL and one BUY.
        """
        if not raw_transactions:
            return []
            
        normalized = []
        
        for txn in raw_transactions:
            try:
                # Skip non-swap transactions
                if txn.get('tx_type') != 'swap':
                    continue
                    
                # Get swap direction from type_swap fields
                base_type = txn.get('base_type_swap')
                quote_type = txn.get('quote_type_swap')
                
                # Skip if we can't determine direction
                if not base_type or not quote_type:
                    continue
                
                # Extract amounts and prices
                base_amount = abs(float(txn.get('base_ui_change_amount') or 0))
                quote_amount = abs(float(txn.get('quote_ui_change_amount') or 0))
                base_price = float(txn.get('base_nearest_price') or 0)
                quote_price = float(txn.get('quote_nearest_price') or 0)
                
                # Skip if amounts are invalid
                if base_amount == 0 or quote_amount == 0:
                    continue
                
                timestamp = txn.get('timestamp')
                tx_hash = txn.get('transaction_hash')
                
                # Determine which token was sold and which was bought
                if base_type == 'from' and quote_type == 'to':
                    # Selling base for quote
                    # SELL record for base token
                    if base_price > 0:
                        normalized.append({
                            'token_address': txn.get('base_address'),
                            'transaction_type': 'SELL',
                            'amount': base_amount,
                            'price': base_price,
                            'value_usd': base_amount * base_price,
                            'timestamp': timestamp,
                            'transaction_hash': tx_hash
                        })
                    
                    # BUY record for quote token
                    if quote_price > 0:
                        normalized.append({
                            'token_address': txn.get('quote_address'),
                            'transaction_type': 'BUY',
                            'amount': quote_amount,
                            'price': quote_price,
                            'value_usd': quote_amount * quote_price,
                            'timestamp': timestamp,
                            'transaction_hash': tx_hash
                        })
                        
                elif base_type == 'to' and quote_type == 'from':
                    # Selling quote for base
                    # SELL record for quote token
                    if quote_price > 0:
                        normalized.append({
                            'token_address': txn.get('quote_address'),
                            'transaction_type': 'SELL',
                            'amount': quote_amount,
                            'price': quote_price,
                            'value_usd': quote_amount * quote_price,
                            'timestamp': timestamp,
                            'transaction_hash': tx_hash
                        })
                    
                    # BUY record for base token
                    if base_price > 0:
                        normalized.append({
                            'token_address': txn.get('base_address'),
                            'transaction_type': 'BUY',
                            'amount': base_amount,
                            'price': base_price,
                            'value_usd': base_amount * base_price,
                            'timestamp': timestamp,
                            'transaction_hash': tx_hash
                        })
                        
            except Exception as e:
                # Skip problematic transactions
                continue
                
        return normalized
    
    # Define improved FIFO UDF for normalized transactions
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
        FIFO PnL calculation for normalized transactions.
        Expects transactions with: token_address, transaction_type, amount, price, value_usd, timestamp
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
                amount = float(txn.get('amount') or 0)
                price = float(txn.get('price') or 0)
                value_usd = float(txn.get('value_usd') or 0)
                timestamp = txn.get('timestamp')
                
                # Handle timestamp conversion
                if hasattr(timestamp, 'timestamp'):
                    timestamp_unix = timestamp.timestamp()
                else:
                    timestamp_unix = float(timestamp) if timestamp else 0
                
                # Track latest price
                if price > 0:
                    latest_price = price
                
                # Skip invalid transactions
                if amount == 0 or price == 0:
                    continue
                
                # Enhanced BUY processing
                if tx_type == 'BUY':
                    # Calculate cost basis
                    cost_basis = value_usd if value_usd > 0 else (amount * price)
                    buy_price = cost_basis / amount if amount > 0 else price
                    
                    total_bought += cost_basis
                    
                    # Add to buy lots queue
                    buy_lots.append({
                        'amount': amount,
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
                            
                            if buy_lot['amount'] <= PNL_AMOUNT_PRECISION_THRESHOLD:  # Almost zero, remove
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
                elif tx_type == 'SELL':
                    # Calculate sale value
                    sale_value = value_usd if value_usd > 0 else (amount * price)
                    sell_price = sale_value / amount if amount > 0 else price
                    
                    total_sold += sale_value
                    remaining_to_sell = amount
                    
                    # Try to match against existing buy lots
                    while remaining_to_sell > PNL_AMOUNT_PRECISION_THRESHOLD and buy_lots:
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
                    if remaining_to_sell > PNL_AMOUNT_PRECISION_THRESHOLD:
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
        # Read raw bronze transactions
        try:
            # Read ALL available CLEAN bronze transaction data for comprehensive analysis
            # Exclude status files which have different schema (INT64 timestamp vs proper timestamp)
            parquet_path = f"s3a://solana-data/{BRONZE_WALLET_TRANSACTIONS_PATH}/**/clean_*.parquet"
            
            bronze_df = spark.read.parquet(parquet_path)
            logger.info(f"Successfully read raw bronze data from all available dates")
            
            # Deduplicate by transaction hash
            deduped_df = bronze_df.dropDuplicates(["wallet_address", "transaction_hash"])
            
            # Filter for swap transactions only
            swap_df = deduped_df.filter(
                (col("tx_type") == "swap") &
                (col("transaction_hash").isNotNull()) &
                (col("wallet_address").isNotNull()) &
                (col("timestamp").isNotNull()) &
                (col("base_address").isNotNull()) &
                (col("quote_address").isNotNull())
            )
            
            # Get unique wallets
            unique_wallets = swap_df.select("wallet_address").distinct().collect()
            wallet_count = len(unique_wallets)
            logger.info(f"Found {wallet_count} unique wallets with swap transactions")
            
            if wallet_count == 0:
                logger.info("No wallets with swap transactions - returning success")
                return {
                    "wallets_processed": 0,
                    "total_pnl_records": 0,
                    "batch_id": batch_id,
                    "status": "no_data"
                }
            
            # Process each wallet
            current_time = datetime.now(timezone.utc)
            week_ago = current_time - timedelta(days=RECENT_DAYS)
            
            portfolio_results = []
            wallets_processed = 0
            
            for wallet_row in unique_wallets:
                wallet_address = wallet_row['wallet_address']
                
                # Get all transactions for this wallet
                wallet_txns = swap_df.filter(col("wallet_address") == wallet_address)
                
                # Apply smart filtering: recent + historical
                recent_txns = wallet_txns.filter(col("timestamp") >= lit(week_ago))
                recent_count = recent_txns.count()
                
                if recent_count < HISTORICAL_LIMIT:
                    # Need more historical context
                    older_txns = wallet_txns.filter(col("timestamp") < lit(week_ago)) \
                        .orderBy(col("timestamp").desc()) \
                        .limit(HISTORICAL_LIMIT - recent_count)
                    
                    final_txns = recent_txns.unionAll(older_txns)
                else:
                    # Just use recent transactions
                    final_txns = recent_txns
                
                # Skip wallets with too few transactions
                total_txn_count = final_txns.count()
                if total_txn_count < MIN_TRANSACTIONS:
                    logger.info(f"Skipping wallet {wallet_address[:10]}... - only {total_txn_count} transactions")
                    continue
                
                # Collect all transactions for this wallet
                wallet_raw_txns = final_txns.orderBy("timestamp").collect()
                
                # Transform raw to normalized format
                wallet_txn_dicts = [row.asDict() for row in wallet_raw_txns]
                normalized_txns = transform_raw_to_normalized.func(wallet_txn_dicts)
                
                if not normalized_txns:
                    logger.warning(f"No valid normalized transactions for wallet {wallet_address[:10]}...")
                    continue
                
                # Group by token and calculate FIFO PnL
                token_pnls = {}
                for norm_txn in normalized_txns:
                    token_addr = norm_txn['token_address']
                    if token_addr not in token_pnls:
                        token_pnls[token_addr] = []
                    token_pnls[token_addr].append(norm_txn)
                
                # Calculate PnL for each token
                portfolio_metrics = {
                    'realized_pnl': 0.0,
                    'unrealized_pnl': 0.0,
                    'total_bought': 0.0,
                    'total_sold': 0.0,
                    'trade_count': 0,
                    'winning_trades': 0,
                    'total_holding_time_hours': 0.0,
                    'current_position_cost_basis': 0.0
                }
                
                for token_addr, token_txns in token_pnls.items():
                    # Sort by timestamp for FIFO
                    token_txns.sort(key=lambda x: x['timestamp'])
                    
                    # Calculate FIFO PnL for this token
                    token_result = calculate_token_pnl_fifo.func(token_txns)
                    
                    # Aggregate into portfolio metrics
                    portfolio_metrics['realized_pnl'] += token_result[0]
                    portfolio_metrics['unrealized_pnl'] += token_result[1]
                    portfolio_metrics['total_bought'] += token_result[2]
                    portfolio_metrics['total_sold'] += token_result[3]
                    portfolio_metrics['trade_count'] += token_result[4]
                    portfolio_metrics['winning_trades'] += token_result[5]
                    portfolio_metrics['total_holding_time_hours'] += token_result[6]
                    portfolio_metrics['current_position_cost_basis'] += token_result[8]
                
                # Calculate portfolio-level derived metrics
                total_pnl = portfolio_metrics['realized_pnl'] + portfolio_metrics['unrealized_pnl']
                win_rate = (portfolio_metrics['winning_trades'] / portfolio_metrics['trade_count'] * 100.0) \
                    if portfolio_metrics['trade_count'] > 0 else 0.0
                roi = ((portfolio_metrics['total_sold'] - portfolio_metrics['total_bought']) / \
                    portfolio_metrics['total_bought'] * 100.0) \
                    if portfolio_metrics['total_bought'] > 0 else 0.0
                avg_holding_time = (portfolio_metrics['total_holding_time_hours'] / \
                    portfolio_metrics['trade_count']) \
                    if portfolio_metrics['trade_count'] > 0 else 0.0
                
                # Get first and last transaction timestamps
                first_txn = min(wallet_raw_txns, key=lambda x: x['timestamp'])
                last_txn = max(wallet_raw_txns, key=lambda x: x['timestamp'])
                
                # Create portfolio record (one per wallet)
                portfolio_record = {
                    'wallet_address': wallet_address,
                    'token_address': 'ALL_TOKENS',  # Portfolio aggregate marker
                    'calculation_date': current_time.date(),
                    'realized_pnl': portfolio_metrics['realized_pnl'],
                    'unrealized_pnl': portfolio_metrics['unrealized_pnl'],
                    'total_pnl': total_pnl,
                    'trade_count': portfolio_metrics['trade_count'],
                    'win_rate': win_rate,
                    'total_bought': portfolio_metrics['total_bought'],
                    'total_sold': portfolio_metrics['total_sold'],
                    'roi': roi,
                    'avg_holding_time_hours': avg_holding_time,
                    'avg_transaction_amount_usd': (portfolio_metrics['total_bought'] + \
                        portfolio_metrics['total_sold']) / (portfolio_metrics['trade_count'] * 2) \
                        if portfolio_metrics['trade_count'] > 0 else 0.0,
                    'trade_frequency_daily': portfolio_metrics['trade_count'] / \
                        max(1, (last_txn['timestamp'] - first_txn['timestamp']).total_seconds() / 86400),
                    'first_transaction': first_txn['timestamp'],
                    'last_transaction': last_txn['timestamp'],
                    'current_position_tokens': 0.0,  # Aggregated across all tokens
                    'current_position_cost_basis': portfolio_metrics['current_position_cost_basis'],
                    'current_position_value': 0.0,  # Would need current prices
                    'processed_at': current_time,
                    'batch_id': batch_id,
                    'data_source': 'birdeye_v3'
                }
                
                portfolio_results.append(portfolio_record)
                wallets_processed += 1
                
                if wallets_processed % PNL_BATCH_PROGRESS_INTERVAL == 0:
                    logger.info(f"Processed {wallets_processed} wallets...")
            
            # Convert results to DataFrame and write to silver layer
            if portfolio_results:
                logger.info(f"Writing {len(portfolio_results)} portfolio records to silver layer")
                
                # Define simplified schema for portfolio results (removed gold processing columns)
                portfolio_schema = StructType([
                    StructField("wallet_address", StringType(), False),
                    StructField("token_address", StringType(), False),
                    StructField("calculation_date", DateType(), False),
                    StructField("realized_pnl", DoubleType(), False),
                    StructField("unrealized_pnl", DoubleType(), False),
                    StructField("total_pnl", DoubleType(), False),
                    StructField("trade_count", IntegerType(), False),
                    StructField("win_rate", DoubleType(), False),
                    StructField("total_bought", DoubleType(), False),
                    StructField("total_sold", DoubleType(), False),
                    StructField("roi", DoubleType(), False),
                    StructField("avg_holding_time_hours", DoubleType(), False),
                    StructField("avg_transaction_amount_usd", DoubleType(), False),
                    StructField("trade_frequency_daily", DoubleType(), False),
                    StructField("first_transaction", TimestampType(), True),
                    StructField("last_transaction", TimestampType(), True),
                    StructField("current_position_tokens", DoubleType(), False),
                    StructField("current_position_cost_basis", DoubleType(), False),
                    StructField("current_position_value", DoubleType(), False),
                    StructField("processed_at", TimestampType(), False),
                    StructField("batch_id", StringType(), False),
                    StructField("data_source", StringType(), False)
                ])
                
                # Create DataFrame from results with explicit schema
                results_df = spark.createDataFrame(portfolio_results, schema=portfolio_schema)
                
                # Add partitioning columns
                partitioned_df = results_df.withColumn(
                    "calculation_year", expr("year(calculation_date)")
                ).withColumn(
                    "calculation_month", expr("month(calculation_date)")
                )
                
                # Write to silver layer
                output_path = "s3a://solana-data/silver/wallet_pnl/"
                
                partitioned_df.write \
                    .partitionBy("calculation_year", "calculation_month") \
                    .mode("append") \
                    .parquet(output_path)
                
                # Write success marker
                current_date = datetime.now()
                success_path = f"s3a://solana-data/silver/wallet_pnl/calculation_year={current_date.year}/calculation_month={current_date.month}/_SUCCESS_{batch_id}"
                spark.createDataFrame([("success",)], ["status"]).coalesce(1).write.mode("overwrite").text(success_path)
                
                logger.info(f"PnL calculation completed successfully for batch {batch_id}")
                logger.info(f"Processed {wallets_processed} wallets with portfolio aggregation")
                
                return {
                    "wallets_processed": wallets_processed,
                    "total_pnl_records": len(portfolio_results),
                    "batch_id": batch_id,
                    "output_path": output_path,
                    "status": "success"
                }
            else:
                logger.info("No portfolio results generated")
                return {
                    "wallets_processed": 0,
                    "total_pnl_records": 0,
                    "batch_id": batch_id,
                    "status": "no_valid_data"
                }
                
        except Exception as e:
            logger.error(f"Failed to read bronze data: {e}")
            raise
            
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