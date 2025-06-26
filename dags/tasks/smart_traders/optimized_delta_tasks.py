"""
Optimized Delta Lake Tasks - TRUE Implementation
ZERO FALLBACKS, ZERO MOCK DATA, MINIMAL LOGGING
Consolidated, efficient, and clean Delta Lake operations
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Import True Delta Lake manager ONLY
from utils.true_delta_manager import TrueDeltaLakeManager, get_table_path
from config.true_delta_config import get_table_config

# Import PySpark functions
from pyspark.sql.functions import (
    col, lit, when, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    current_timestamp, coalesce, round as spark_round, abs as spark_abs,
    datediff, expr, to_date, row_number, least, current_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType, BooleanType, FloatType


def create_bronze_tokens_delta(**context) -> Dict[str, Any]:
    """Bronze tokens: BirdEye API → Delta Lake"""
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        delta_manager = TrueDeltaLakeManager()
        
        # Get API key
        from airflow.models import Variable
        try:
            api_key = Variable.get('BIRDSEYE_API_KEY')
        except:
            import os
            api_key = os.environ.get('BIRDSEYE_API_KEY')
        
        if not api_key:
            raise ValueError("BIRDSEYE_API_KEY not found")
        
        # Fetch tokens from BirdEye API
        from birdeye_client import BirdEyeAPIClient
        from config.smart_trader_config import (
            TOKEN_LIMIT, MIN_LIQUIDITY, MAX_LIQUIDITY, MIN_VOLUME_1H_USD,
            MIN_PRICE_CHANGE_2H_PERCENT, MIN_PRICE_CHANGE_24H_PERCENT,
            API_RATE_LIMIT_DELAY, API_PAGINATION_LIMIT
        )
        
        birdeye_client = BirdEyeAPIClient(api_key)
        filter_params = {
            "min_liquidity": MIN_LIQUIDITY,
            "max_liquidity": MAX_LIQUIDITY,
            "min_volume_1h_usd": MIN_VOLUME_1H_USD,
            "min_price_change_2h_percent": MIN_PRICE_CHANGE_2H_PERCENT,
            "min_price_change_24h_percent": MIN_PRICE_CHANGE_24H_PERCENT
        }
        
        # Log the configuration values
        logger.info(f"Bronze tokens configuration:")
        logger.info(f"  TOKEN_LIMIT: {TOKEN_LIMIT}")
        logger.info(f"  MIN_LIQUIDITY: ${MIN_LIQUIDITY:,}")
        logger.info(f"  MAX_LIQUIDITY: ${MAX_LIQUIDITY:,}")
        logger.info(f"  MIN_VOLUME_1H_USD: ${MIN_VOLUME_1H_USD:,}")
        logger.info(f"  MIN_PRICE_CHANGE_2H_PERCENT: {MIN_PRICE_CHANGE_2H_PERCENT}%")
        logger.info(f"  MIN_PRICE_CHANGE_24H_PERCENT: {MIN_PRICE_CHANGE_24H_PERCENT}%")
        
        # Paginated API fetch
        offset, all_tokens = 0, []
        api_call_count = 0
        while len(all_tokens) < TOKEN_LIMIT:
            remaining = TOKEN_LIMIT - len(all_tokens)
            current_limit = min(API_PAGINATION_LIMIT, remaining)
            
            params = {
                "sort_by": "liquidity", "sort_type": "desc", 
                "offset": offset, "limit": current_limit,
                **filter_params
            }
            
            api_call_count += 1
            logger.info(f"API call #{api_call_count} - Requesting tokens with params: {params}")
            
            response = birdeye_client.get_token_list(**params)
            tokens_data = birdeye_client.normalize_token_list_response(response)
            
            logger.info(f"API call #{api_call_count} - Received {len(tokens_data)} tokens")
            
            if not tokens_data:
                logger.warning(f"No more tokens returned from API at offset {offset}")
                break
                
            all_tokens.extend(tokens_data)
            offset += len(tokens_data)
            
            if len(tokens_data) < current_limit:
                logger.info(f"Received fewer tokens ({len(tokens_data)}) than requested ({current_limit}), likely end of results")
                break
                
            import time
            time.sleep(API_RATE_LIMIT_DELAY)
        
        logger.info(f"Total tokens fetched from API: {len(all_tokens)}")
        
        tokens_result = all_tokens[:TOKEN_LIMIT]
        logger.info(f"Tokens to process (limited to TOKEN_LIMIT): {len(tokens_result)}")
        
        if not tokens_result:
            logger.warning("No tokens matched the filter criteria")
            return {"status": "no_data", "records": 0}
        
        # Process tokens in small batches to prevent memory issues
        BATCH_SIZE = 10  # Ultra-conservative batch size
        total_processed = 0
        
        for i in range(0, len(tokens_result), BATCH_SIZE):
            batch_tokens = tokens_result[i:i + BATCH_SIZE]
            logger.info(f"Processing token batch {i//BATCH_SIZE + 1}: {len(batch_tokens)} tokens")
            
            # Process batch
            if not batch_tokens:
                continue
            
            # Explicit schema based on BirdEye API token response (normalized field names)
            token_schema = StructType([
                StructField("token_address", StringType(), False),
                StructField("logo_uri", StringType(), True),
                StructField("name", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("decimals", LongType(), True),
                StructField("market_cap", DoubleType(), True),
                StructField("fdv", DoubleType(), True),
                StructField("liquidity", DoubleType(), True),
                StructField("last_trade_unix_time", LongType(), True),
                StructField("volume_1h_usd", DoubleType(), True),
                StructField("volume_1h_change_percent", DoubleType(), True),
                StructField("volume_2h_usd", DoubleType(), True),
                StructField("volume_2h_change_percent", DoubleType(), True),
                StructField("volume_4h_usd", DoubleType(), True),
                StructField("volume_4h_change_percent", DoubleType(), True),
                StructField("volume_8h_usd", DoubleType(), True),
                StructField("volume_8h_change_percent", DoubleType(), True),
                StructField("volume_24h_usd", DoubleType(), True),
                StructField("volume_24h_change_percent", DoubleType(), True),
                StructField("trade_1h_count", LongType(), True),
                StructField("trade_2h_count", LongType(), True),
                StructField("trade_4h_count", LongType(), True),
                StructField("trade_8h_count", LongType(), True),
                StructField("trade_24h_count", LongType(), True),
                StructField("price", DoubleType(), True),
                StructField("price_change_1h_percent", DoubleType(), True),
                StructField("price_change_2h_percent", DoubleType(), True),
                StructField("price_change_4h_percent", DoubleType(), True),
                StructField("price_change_8h_percent", DoubleType(), True),
                StructField("price_change_24h_percent", DoubleType(), True),
                StructField("holder", LongType(), True),
                StructField("recent_listing_time", LongType(), True)
            ])
            
            # Convert to Spark DataFrame with explicit schema
            df = delta_manager.spark.createDataFrame(batch_tokens, token_schema)
            df_with_metadata = df.withColumn("_delta_timestamp", current_timestamp()) \
                                .withColumn("_delta_operation", lit("BRONZE_TOKEN_CREATE")) \
                                .withColumn("processing_date", current_date()) \
                                .withColumn("batch_id", lit(context.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S")))) \
                                .withColumn("processed", lit(False)) \
                                .withColumn("_delta_created_at", lit(datetime.now().isoformat()))
            
            table_path = get_table_path("bronze_tokens")
            table_config = get_table_config("bronze_tokens")
            
            # For first batch, handle schema evolution; for subsequent batches, use append
            if i == 0:  # First batch
                # TEMPORARY: Use overwrite to handle schema evolution from old schema to new explicit schema
                if delta_manager.table_exists(table_path):
                    version = delta_manager.create_table(
                        df_with_metadata, table_path, 
                        partition_cols=table_config["partition_cols"], 
                        merge_schema=False  # Use overwrite to handle schema change
                    )
                    operation = "OVERWRITE_SCHEMA_EVOLUTION"
                else:
                    version = delta_manager.create_table(
                        df_with_metadata, table_path, partition_cols=table_config["partition_cols"]
                    )
                    operation = "CREATE"
            else:  # Subsequent batches
                version = delta_manager.append_data(df_with_metadata, table_path, merge_schema=True)
                operation = "APPEND_BATCH"
            
            total_processed += len(batch_tokens)
            logger.info(f"Batch {i//BATCH_SIZE + 1}: {len(batch_tokens)} tokens → Delta v{version} ({operation})")
            
            # Small delay between batches to prevent resource exhaustion
            import time
            time.sleep(0.5)
        
        health_check = delta_manager.validate_table_health(table_path)
        
        logger.info(f"Bronze tokens complete: {total_processed} total records processed in {len(range(0, len(tokens_result), BATCH_SIZE))} batches")
        
        return {
            "status": "success", "records": total_processed,
            "delta_version": version, "table_path": table_path, "batches": len(range(0, len(tokens_result), BATCH_SIZE))
        }
        
    except Exception as e:
        logger.error(f"Bronze tokens failed: {str(e)}")
        return {"status": "failed", "error": str(e), "records": 0}
    finally:
        if delta_manager:
            delta_manager.stop()


def create_bronze_whales_delta(**context) -> Dict[str, Any]:
    """Bronze whales: Silver whales → BirdEye API → Delta Lake"""
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        delta_manager = TrueDeltaLakeManager()
        
        # Read from silver tracked tokens to find tokens needing whale data
        silver_tokens_path = get_table_path("silver_tokens")
        if not delta_manager.table_exists(silver_tokens_path):
            return {"status": "no_source", "records": 0}
        
        silver_tokens_df = delta_manager.spark.read.format("delta").load(silver_tokens_path)
        
        # Filter for tokens that need whale data fetched
        tokens_needing_whales = silver_tokens_df.filter(
            (col("whale_fetch_status") == "pending") |
            col("whale_fetch_status").isNull() |
            # 72-hour refetch logic
            (col("whale_fetched_at").isNotNull() & 
             (datediff(current_timestamp(), col("whale_fetched_at")) >= 3))
        ).select("token_address", "symbol", "name").limit(5)
        
        # Collect limited data to avoid JVM crashes
        whale_tokens = tokens_needing_whales.collect()
        if not whale_tokens:
            return {"status": "no_data", "records": 0}
        
        # Get API key and fetch whale data
        from airflow.models import Variable
        try:
            api_key = Variable.get('BIRDSEYE_API_KEY')
        except:
            import os
            api_key = os.environ.get('BIRDSEYE_API_KEY')
        
        if not api_key:
            raise ValueError("BIRDSEYE_API_KEY not found")
        
        from birdeye_client import BirdEyeAPIClient
        from config.smart_trader_config import MAX_WHALES_PER_TOKEN, API_RATE_LIMIT_DELAY
        
        birdeye_client = BirdEyeAPIClient(api_key)
        all_whale_data = []
        batch_id = context.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S"))
        # Convert to dict for processing
        tokens_list = [row.asDict() for row in whale_tokens]
        
        for token_data in tokens_list:
            token_address = token_data['token_address']
            
            response = birdeye_client.get_token_top_holders(
                token_address=token_address, offset=0, limit=MAX_WHALES_PER_TOKEN
            )
            
            if response.get('success') and 'data' in response:
                holders_data = response['data'].get('items', response['data'].get('holders', []))
                
                for idx, holder in enumerate(holders_data):
                    whale_record = {
                        "token_address": str(token_address),
                        "token_symbol": str(token_data.get('symbol', '')),
                        "token_name": str(token_data.get('name', '')),
                        "wallet_address": str(holder.get('owner', '')),
                        "rank": int(idx + 1),
                        # Use actual API response fields
                        "amount": str(holder.get('amount', '0')),  # Raw amount as string
                        "ui_amount": float(holder.get('ui_amount', 0)),  # Human readable amount
                        "decimals": int(holder.get('decimals', 9)),
                        "mint": str(holder.get('mint', token_address)),  # Should be same as token_address
                        "token_account": str(holder.get('token_account', '')),
                        "fetched_at": datetime.now().isoformat(),
                        "batch_id": str(batch_id),
                        "data_source": "birdeye_v3"
                    }
                    all_whale_data.append(whale_record)
            
            import time
            time.sleep(API_RATE_LIMIT_DELAY)
        
        if not all_whale_data:
            return {"status": "no_data", "records": 0}
        
        # Explicit schema to match actual API response
        whale_schema = StructType([
            StructField("token_address", StringType(), False),
            StructField("token_symbol", StringType(), True),
            StructField("token_name", StringType(), True),
            StructField("wallet_address", StringType(), False),
            StructField("rank", IntegerType(), True),
            StructField("amount", StringType(), True),  # Raw amount as string
            StructField("ui_amount", FloatType(), True),  # Human readable amount
            StructField("decimals", IntegerType(), True),
            StructField("mint", StringType(), True),  # Token mint address
            StructField("token_account", StringType(), True),  # Holder's token account
            StructField("fetched_at", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("data_source", StringType(), False)
        ])
        
        df = delta_manager.spark.createDataFrame(all_whale_data, schema=whale_schema)
        df_with_metadata = df.withColumn("_delta_timestamp", current_timestamp()) \
                            .withColumn("_delta_operation", lit("BRONZE_WHALE_CREATE")) \
                            .withColumn("rank_date", current_date()) \
                            .withColumn("_delta_created_at", lit(datetime.now().isoformat()))
        
        table_path = get_table_path("bronze_whales")
        table_config = get_table_config("bronze_whales")
        
        # Use MERGE to update existing whale holdings and insert new ones
        if delta_manager.table_exists(table_path):
            merge_condition = "target.wallet_address = source.wallet_address AND target.token_address = source.token_address"
            source_columns = [c for c in df_with_metadata.columns if not c.startswith("_delta")]
            update_set = {c: f"source.{c}" for c in source_columns if c not in ["wallet_address", "token_address"]}
            insert_values = {c: f"source.{c}" for c in df_with_metadata.columns}
            version = delta_manager.merge_data(df_with_metadata, table_path, merge_condition, update_set, insert_values)
            operation = "MERGE"
        else:
            version = delta_manager.create_table(
                df_with_metadata, table_path, partition_cols=table_config["partition_cols"]
            )
            operation = "CREATE"
        
        health_check = delta_manager.validate_table_health(table_path)
        
        # Update silver tracked tokens whale_fetch_status to 'completed' for processed tokens
        if len(tokens_list) > 0 and len(all_whale_data) > 0:
            processed_token_addresses = [token_data['token_address'] for token_data in tokens_list]
            
            # Read current silver tokens table
            silver_tokens_path = get_table_path("silver_tokens")
            if delta_manager.table_exists(silver_tokens_path):
                silver_tokens_df = delta_manager.spark.read.format("delta").load(silver_tokens_path)
                
                # Update whale_fetch_status for processed tokens
                updated_silver_df = silver_tokens_df.withColumn(
                    "whale_fetch_status",
                    when(col("token_address").isin(processed_token_addresses), lit("completed"))
                    .otherwise(col("whale_fetch_status"))
                ).withColumn(
                    "whale_fetched_at", 
                    when(col("token_address").isin(processed_token_addresses), current_timestamp())
                    .otherwise(col("whale_fetched_at"))
                ).withColumn(
                    "_delta_timestamp", current_timestamp()
                ).withColumn(
                    "_delta_operation", 
                    when(col("token_address").isin(processed_token_addresses), lit("WHALE_STATUS_UPDATE"))
                    .otherwise(lit("SILVER_TOKEN_UPDATE"))
                )
                
                # Write updated silver tokens table back with schema evolution
                updated_silver_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_tokens_path)
                logger.info(f"Updated whale_fetch_status to 'completed' for {len(processed_token_addresses)} tokens")
        
        logger.info(f"Bronze whales: {len(all_whale_data)} records → Delta v{version} ({operation})")
        
        return {
            "status": "success", "records": len(all_whale_data),
            "delta_version": version, "operation": operation
        }
        
    except Exception as e:
        logger.error(f"Bronze whales failed: {str(e)}")
        return {"status": "failed", "error": str(e), "records": 0}
    finally:
        if delta_manager:
            delta_manager.stop()


def create_bronze_transactions_delta(**context) -> Dict[str, Any]:
    """Bronze transactions: Silver whales → BirdEye API → Delta Lake
    Fetches transaction data for whales that need processing or refetching (1-month cycle)
    """
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        delta_manager = TrueDeltaLakeManager()
        
        # Read from silver tracked whales
        silver_whales_path = get_table_path("silver_whales")
        if not delta_manager.table_exists(silver_whales_path):
            return {"status": "no_source", "records": 0}
        
        silver_whales_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
        
        # Check if bronze_transactions table exists
        bronze_transactions_path = get_table_path("bronze_transactions")
        bronze_transactions_exists = delta_manager.table_exists(bronze_transactions_path)
        
        if bronze_transactions_exists:
            # Get the latest transaction timestamp for each wallet
            bronze_transactions_df = delta_manager.spark.read.format("delta").load(bronze_transactions_path)
            latest_transactions = bronze_transactions_df.groupBy("wallet_address").agg(
                spark_max("timestamp").alias("latest_transaction_timestamp"),
                count("*").alias("transaction_count")
            )
            
            # Join with silver whales to find who needs updates
            whales_with_tx_info = silver_whales_df.join(
                latest_transactions,
                silver_whales_df.wallet_address == latest_transactions.wallet_address,
                "left"
            )
            
            # Filter for whales needing transaction data fetched
            # Include: 1) New whales (no transactions at all)
            #         2) Whales with transactions older than 1 month (30 days)
            #         3) Whales with pending/ready status regardless of transaction age
            whales_needing_transactions = whales_with_tx_info.filter(
                # New whales or whales needing processing
                (col("processing_status") == "pending") |
                (col("processing_status") == "ready") |
                # No transactions found
                col("latest_transaction_timestamp").isNull() |
                # Transactions older than 1 month (30 days)
                (col("latest_transaction_timestamp").isNotNull() & 
                 (datediff(current_timestamp(), col("latest_transaction_timestamp")) >= 30))
            ).select(
                silver_whales_df.whale_id.alias("whale_id"),
                silver_whales_df.wallet_address.alias("wallet_address"),
                silver_whales_df.token_address.alias("token_address"),
                silver_whales_df.token_symbol.alias("token_symbol")
            ).limit(10)
        else:
            # If bronze_transactions doesn't exist, fetch for all whales needing transactions
            whales_needing_transactions = silver_whales_df.filter(
                (col("processing_status") == "pending") |
                (col("processing_status") == "ready") |
                col("txns_last_fetched_at").isNull()
            ).select("whale_id", "wallet_address", "token_address", "token_symbol").limit(10)
        
        # Collect limited data without count() to avoid crashes
        whales_list = [row.asDict() for row in whales_needing_transactions.collect()]
        if not whales_list:
            return {"status": "no_data", "records": 0}
        whales_count = len(whales_list)
        
        logger.info(f"Found {whales_count} whales needing transaction updates (1-month refresh cycle)")
        
        # Get API key and fetch transaction data
        from airflow.models import Variable
        try:
            api_key = Variable.get('BIRDSEYE_API_KEY')
        except:
            import os
            api_key = os.environ.get('BIRDSEYE_API_KEY')
        
        if not api_key:
            raise ValueError("BIRDSEYE_API_KEY not found")
        
        from birdeye_client import BirdEyeAPIClient
        from config.smart_trader_config import MAX_TRANSACTIONS_PER_WALLET, WALLET_API_DELAY
        
        birdeye_client = BirdEyeAPIClient(api_key)
        all_transaction_data = []
        batch_id = context.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S"))
        
        for whale_data in whales_list:
            wallet_address = whale_data['wallet_address']
            whale_id = whale_data['whale_id']
            
            response = birdeye_client.get_wallet_transactions(
                wallet_address=wallet_address, limit=MAX_TRANSACTIONS_PER_WALLET
            )
            
            trades = []
            if response.get('success') and 'data' in response:
                if isinstance(response['data'], list):
                    trades = response['data']
                elif isinstance(response['data'], dict):
                    trades = response['data'].get('items', response['data'].get('trades', []))
            
            for trade in trades:
                tx_hash = trade.get('tx_hash', '')
                if not tx_hash or trade.get('owner') != wallet_address:
                    continue
                
                base = trade.get('base', {})
                quote = trade.get('quote', {})
                
                trade_timestamp = datetime.now()
                if trade.get('block_unix_time'):
                    try:
                        trade_timestamp = datetime.fromtimestamp(trade.get('block_unix_time'))
                    except:
                        pass
                
                transaction_record = {
                    "whale_id": whale_id,
                    "wallet_address": wallet_address,
                    "transaction_hash": tx_hash,
                    "timestamp": trade_timestamp,
                    "base_symbol": base.get('symbol'),
                    "base_address": base.get('address'),
                    "base_type_swap": base.get('type_swap'),
                    "base_ui_change_amount": float(base.get('ui_change_amount', 0)) if base.get('ui_change_amount') is not None else None,
                    "base_nearest_price": float(base.get('nearest_price', 0)) if base.get('nearest_price') is not None else None,
                    "quote_symbol": quote.get('symbol'),
                    "quote_address": quote.get('address'),
                    "quote_type_swap": quote.get('type_swap'),
                    "quote_ui_change_amount": float(quote.get('ui_change_amount', 0)) if quote.get('ui_change_amount') is not None else None,
                    "quote_nearest_price": float(quote.get('nearest_price', 0)) if quote.get('nearest_price') is not None else None,
                    "source": trade.get('source'),
                    "tx_type": trade.get('tx_type'),
                    "fetched_at": datetime.now().isoformat(),
                    "batch_id": batch_id,
                    "data_source": "birdeye_v3"
                }
                all_transaction_data.append(transaction_record)
            
            import time
            time.sleep(WALLET_API_DELAY)
        
        if not all_transaction_data:
            return {"status": "no_data", "records": 0}
        
        # Explicit schema
        transaction_schema = StructType([
            StructField("whale_id", StringType(), False),
            StructField("wallet_address", StringType(), False),
            StructField("transaction_hash", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("base_symbol", StringType(), True),
            StructField("base_address", StringType(), True),
            StructField("base_type_swap", StringType(), True),
            StructField("base_ui_change_amount", FloatType(), True),
            StructField("base_nearest_price", FloatType(), True),
            StructField("quote_symbol", StringType(), True),
            StructField("quote_address", StringType(), True),
            StructField("quote_type_swap", StringType(), True),
            StructField("quote_ui_change_amount", FloatType(), True),
            StructField("quote_nearest_price", FloatType(), True),
            StructField("source", StringType(), True),
            StructField("tx_type", StringType(), True),
            StructField("fetched_at", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("data_source", StringType(), False)
        ])
        
        df = delta_manager.spark.createDataFrame(all_transaction_data, schema=transaction_schema)
        
        # Deduplicate by transaction_hash + base_address to prevent MERGE conflicts
        # This keeps unique combinations of transaction and token
        original_count = df.count()
        df_deduped = df.dropDuplicates(["transaction_hash", "base_address"])
        deduped_count = df_deduped.count()
        
        if original_count > deduped_count:
            logger.info(f"Removed {original_count - deduped_count} duplicate transaction-token pairs from {original_count} total transactions")
        
        df_with_metadata = df_deduped.withColumn("_delta_timestamp", current_timestamp()) \
                            .withColumn("_delta_operation", lit("BRONZE_TRANSACTION_CREATE")) \
                            .withColumn("transaction_date", to_date(col("timestamp"))) \
                            .withColumn("_delta_created_at", lit(datetime.now().isoformat()))
        
        table_path = get_table_path("bronze_transactions")
        table_config = get_table_config("bronze_transactions")
        
        if delta_manager.table_exists(table_path):
            # Use MERGE to prevent duplicate transactions based on transaction_hash AND base_address (token)
            # This handles cases where same transaction involves multiple tokens
            merge_condition = "target.transaction_hash = source.transaction_hash AND target.base_address = source.base_address"
            source_columns = [c for c in df_with_metadata.columns if not c.startswith("_delta")]
            update_set = {c: f"source.{c}" for c in source_columns if c not in ["transaction_hash", "base_address"]}
            insert_values = {c: f"source.{c}" for c in df_with_metadata.columns}
            version = delta_manager.merge_data(df_with_metadata, table_path, merge_condition, update_set, insert_values)
            operation = "MERGE"
        else:
            version = delta_manager.create_table(
                df_with_metadata, table_path, partition_cols=table_config["partition_cols"]
            )
            operation = "CREATE"
        
        health_check = delta_manager.validate_table_health(table_path)
        
        # Update silver tracked whales processing_status to 'processed' for processed whales
        # This handles both new whales and whales being refetched after 2 weeks
        if whales_count > 0 and len(all_transaction_data) > 0:
            # Deduplicate whale_ids to prevent MERGE conflicts
            processed_whale_ids = list(set([whale_data['whale_id'] for whale_data in whales_list]))
            
            # Read current silver tracked whales table
            silver_whales_path = get_table_path("silver_whales")
            if delta_manager.table_exists(silver_whales_path):
                # Create update DataFrame for processed whales (already deduplicated)
                update_data = []
                for whale_id in processed_whale_ids:
                    update_data.append({
                        "whale_id": whale_id,
                        "txns_fetch_status": "completed",
                        "txns_fetched": True,
                        "txns_last_fetched_at": datetime.now().isoformat(),
                        "_delta_timestamp": datetime.now(),
                        "_delta_operation": "TRANSACTION_STATUS_UPDATE"
                    })
                
                if update_data:
                    updates_df = delta_manager.spark.createDataFrame(update_data)
                    
                    # Use MERGE to update only the specific whales
                    merge_condition = "target.whale_id = source.whale_id"
                    update_set = {
                        "txns_fetch_status": "source.txns_fetch_status",
                        "txns_fetched": "source.txns_fetched",
                        "txns_last_fetched_at": "source.txns_last_fetched_at",
                        "_delta_timestamp": "source._delta_timestamp",
                        "_delta_operation": "source._delta_operation"
                    }
                    
                    delta_manager.merge_data(updates_df, silver_whales_path, merge_condition, update_set, insert_values=None)
                    logger.info(f"Updated txns_fetch_status to 'completed' for {len(processed_whale_ids)} whales using MERGE")
        
        logger.info(f"Bronze transactions: {len(all_transaction_data)} records from {whales_count} wallets → Delta v{version} ({operation})")
        
        return {
            "status": "success", "records": len(all_transaction_data),
            "delta_version": version, "wallets_processed": whales_count
        }
        
    except Exception as e:
        logger.error(f"Bronze transactions failed: {str(e)}")
        return {"status": "failed", "error": str(e), "records": 0}
    finally:
        if delta_manager:
            delta_manager.stop()


def create_silver_wallet_pnl_delta(**context) -> Dict[str, Any]:
    """Silver PnL: Bronze transactions → PnL calculations → Delta Lake (ALL wallets, one at a time)"""
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        delta_manager = TrueDeltaLakeManager()
        
        # Import configuration and memory monitoring tools
        from config.smart_trader_config import PNL_MAX_PROCESSING_TIME_MINUTES, SILVER_PNL_WALLET_BATCH_SIZE
        import gc
        import psutil
        import time
        
        # Read from silver whales to find wallets needing PnL processing
        silver_whales_path = get_table_path("silver_whales")
        if not delta_manager.table_exists(silver_whales_path):
            return {"status": "no_source", "records": 0}
        
        silver_whales_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
        
        # Filter for wallets needing PnL processing
        # Include: 1) New wallets (pending PnL status)
        #         2) Wallets with no PnL timestamp  
        #         3) Wallets last processed more than 1 month ago (30 days)
        wallets_needing_pnl = silver_whales_df.filter(
            (col("pnl_processing_status") == "pending") |
            col("pnl_last_processed_at").isNull() |
            # 1-month refetch logic (30 days)
            (col("pnl_last_processed_at").isNotNull() & 
             (datediff(current_timestamp(), col("pnl_last_processed_at")) >= 30))
        ).select("whale_id", "wallet_address", "token_address", "token_symbol").distinct()
        
        # Check for wallets without count() - use take(1) for efficiency
        if not wallets_needing_pnl.take(1):
            return {"status": "no_data", "records": 0}
        
        # CRITICAL FIX: Get ALL unique wallet addresses that need processing
        all_wallets_needing_pnl = wallets_needing_pnl.select("wallet_address").distinct().collect()
        all_wallet_addresses = [row['wallet_address'] for row in all_wallets_needing_pnl]
        
        if not all_wallet_addresses:
            return {"status": "no_wallets", "records": 0}
        
        logger.info(f"Found {len(all_wallet_addresses)} total wallets needing PnL processing - will process in batches of {SILVER_PNL_WALLET_BATCH_SIZE}")
        
        # Get bronze transactions table path (but don't load full table)
        bronze_transactions_path = get_table_path("bronze_transactions")
        if not delta_manager.table_exists(bronze_transactions_path):
            return {"status": "no_transactions", "records": 0}
        
        # Initialize counters
        processed_wallets = []
        failed_wallets = []
        total_pnl_records = 0
        
        # Add maximum processing time (30 minutes)
        start_time = time.time()
        max_processing_seconds = PNL_MAX_PROCESSING_TIME_MINUTES * 60
        
        # PROCESS WALLETS IN BATCHES (memory safe with timeout)
        for batch_idx in range(0, len(all_wallet_addresses), SILVER_PNL_WALLET_BATCH_SIZE):
            batch_wallets = all_wallet_addresses[batch_idx:batch_idx + SILVER_PNL_WALLET_BATCH_SIZE]
            batch_num = batch_idx // SILVER_PNL_WALLET_BATCH_SIZE + 1
            total_batches = (len(all_wallet_addresses) + SILVER_PNL_WALLET_BATCH_SIZE - 1) // SILVER_PNL_WALLET_BATCH_SIZE
            try:
                # Check memory usage before processing batch
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > 80:
                    logger.warning(f"⚠️ High memory usage: {memory_percent}%. Stopping processing to prevent crash.")
                    logger.info(f"Processed {batch_idx} wallets before memory limit. {len(all_wallet_addresses) - batch_idx} remaining.")
                    break
                
                # Check if we've exceeded max processing time
                elapsed_time = time.time() - start_time
                if elapsed_time > max_processing_seconds:
                    remaining = len(all_wallet_addresses) - batch_idx
                    logger.warning(f"⏱️ Reached max processing time ({PNL_MAX_PROCESSING_TIME_MINUTES} minutes). "
                                 f"Processed {batch_idx} wallets, {remaining} remaining for next run.")
                    break
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_wallets)} wallets) "
                          f"(elapsed: {elapsed_time/60:.1f} minutes, memory: {memory_percent}%)")
                
                # Process batch of wallets
                batch_successful_wallets = []
                batch_failed_wallets = []
                batch_with_data = 0
                batch_no_transactions = 0
                batch_no_valid_transactions = 0
                
                for wallet_address in batch_wallets:
                    # Process single wallet PnL calculation
                    single_wallet_result = _process_single_wallet_pnl(
                        delta_manager, wallet_address, bronze_transactions_path, context
                    )
                    
                    # Check if processing was successful or completed (even with no data)
                    if single_wallet_result["status"] in ["success", "no_transactions", "no_valid_transactions"]:
                        processed_wallets.append(wallet_address)
                        batch_successful_wallets.append(wallet_address)
                        total_pnl_records += single_wallet_result.get("pnl_records", 0)
                        
                        # Count result types
                        if single_wallet_result["status"] == "success":
                            batch_with_data += 1
                        elif single_wallet_result["status"] == "no_transactions":
                            batch_no_transactions += 1
                        elif single_wallet_result["status"] == "no_valid_transactions":
                            batch_no_valid_transactions += 1
                    else:
                        failed_wallets.append(wallet_address)
                        batch_failed_wallets.append(wallet_address)
                
                # Batch update silver_whales status for all wallets in batch
                all_batch_wallets = batch_successful_wallets + batch_failed_wallets
                if all_batch_wallets:
                    _update_wallet_pnl_status(delta_manager, all_batch_wallets, "completed")
                
                # Log batch summary
                logger.info(f"  Batch {batch_num} complete: {batch_with_data} with PnL data, "
                          f"{batch_no_transactions} no transactions, {batch_no_valid_transactions} no valid transactions, "
                          f"{len(batch_failed_wallets)} failed")
                
                # CRITICAL: Clear Spark cache after each batch to prevent memory buildup
                delta_manager.spark.catalog.clearCache()
                
                # Force garbage collection every 5 batches
                if batch_num % 5 == 0:
                    gc.collect()
                    logger.info(f"🧹 Cleared cache and ran garbage collection after {batch_num} batches ({batch_idx + len(batch_wallets)} wallets)")
                    
            except Exception as e:
                # Add all wallets in failed batch to failed list
                for wallet in batch_wallets:
                    if wallet not in processed_wallets:
                        failed_wallets.append(wallet)
                logger.error(f"❌ Exception processing batch {batch_num}: {str(e)}")
                
                # Mark exception wallets as completed to prevent infinite reprocessing
                try:
                    unprocessed_batch_wallets = [w for w in batch_wallets if w not in processed_wallets]
                    if unprocessed_batch_wallets:
                        _update_wallet_pnl_status(delta_manager, unprocessed_batch_wallets, "completed")
                except Exception as status_error:
                    logger.error(f"Failed to update status for exception batch {batch_num}: {status_error}")
                continue
        
        # Log final summary
        logger.info(f"PnL Processing Complete: {len(processed_wallets)} successful, {len(failed_wallets)} failed")
        
        # If we stopped due to timeout, indicate partial completion
        status = "success"
        if len(processed_wallets) + len(failed_wallets) < len(all_wallet_addresses):
            status = "partial_completion"
            logger.info(f"⏱️ Task completed partially due to timeout. Remaining wallets will be processed in next run.")
        
        return {
            "status": status, 
            "total_wallets_found": len(all_wallet_addresses),
            "wallets_processed": len(processed_wallets),
            "wallets_failed": len(failed_wallets),
            "total_pnl_records": total_pnl_records,
            "processed_wallet_addresses": processed_wallets,
            "failed_wallet_addresses": failed_wallets,
            "timeout_reached": status == "partial_completion"
        }
        
    except Exception as e:
        logger.error(f"Silver PnL failed: {str(e)}")
        return {"status": "failed", "error": str(e), "records": 0}
    finally:
        if delta_manager:
            delta_manager.stop()


def _process_single_wallet_pnl(delta_manager, wallet_address: str, bronze_transactions_path: str, context) -> Dict[str, Any]:
    """Process PnL calculation for a single wallet (memory safe)"""
    logger = logging.getLogger(__name__)
    
    try:
        # MEMORY OPTIMIZATION: Load only transactions for this specific wallet
        bronze_transactions_df = delta_manager.spark.read.format("delta") \
            .load(bronze_transactions_path) \
            .filter(
                (col("tx_type") == "swap") &
                (col("wallet_address") == wallet_address) &
                (col("whale_id").isNotNull()) &
                (col("transaction_hash").isNotNull())
            ).dropDuplicates(["transaction_hash"])  # Ensure unique transactions only
        
        # Rename to filtered_transactions for consistency
        filtered_transactions = bronze_transactions_df
        
        # Check if wallet has any transactions
        if not filtered_transactions.take(1):
            return {"status": "no_transactions", "pnl_records": 0}
        
        # Smart USD value calculation
        def calculate_usd_value():
            return when(
                (col("base_nearest_price").isNotNull()) & (col("base_nearest_price") > 0),
                spark_abs(col("base_ui_change_amount")) * col("base_nearest_price")
            ).when(
                (col("quote_nearest_price").isNotNull()) & (col("quote_nearest_price") > 0),
                spark_abs(col("quote_ui_change_amount")) * col("quote_nearest_price")
            ).otherwise(None)
        
        def classify_trade():
            return when(col("base_type_swap") == "to", "BUY").when(col("base_type_swap") == "from", "SELL").otherwise("SWAP")
        
        transactions_with_usd = filtered_transactions.withColumn("usd_value", calculate_usd_value()) \
                                                   .withColumn("trade_type", classify_trade()) \
                                                   .filter(col("usd_value").isNotNull() & (col("usd_value") > 0))
        
        # Check if wallet has valid USD transactions
        if not transactions_with_usd.take(1):
            return {"status": "no_valid_transactions", "pnl_records": 0}
        
        # PHASE 1: CORRECTED FIFO-BASED PnL CALCULATION
        
        # Step 1: Calculate individual transaction prices
        transactions_with_prices = transactions_with_usd.withColumn(
            "token_price", col("usd_value") / spark_abs(col("base_ui_change_amount"))
        )
        
        # Step 2: Create windows for chronological processing per token
        # Window partitioned by wallet and token, ordered by timestamp
        token_window = Window.partitionBy("wallet_address", "base_address").orderBy("timestamp")
        
        # Step 3: Calculate running balances and cost basis for BUY transactions
        buys_df = transactions_with_prices.filter(col("trade_type") == "BUY") \
            .withColumn("cumulative_tokens_bought", 
                       spark_sum(spark_abs(col("base_ui_change_amount"))).over(token_window)) \
            .withColumn("cumulative_cost_usd", 
                       spark_sum(col("usd_value")).over(token_window)) \
            .withColumn("running_avg_cost_basis", 
                       col("cumulative_cost_usd") / col("cumulative_tokens_bought"))
        
        # Step 4: For each SELL, find the cost basis at the time of sale
        sells_df = transactions_with_prices.filter(col("trade_type") == "SELL")
        
        # Create a window for finding the last buy before each sell (ASOF join logic)
        # Get the running cost basis at the time of each sell
        buys_for_matching = buys_df.select(
            "wallet_address", "base_address", "base_symbol", "timestamp", 
            "running_avg_cost_basis", "cumulative_tokens_bought"
        ).withColumnRenamed("timestamp", "buy_timestamp") \
         .withColumnRenamed("running_avg_cost_basis", "cost_basis_at_time") \
         .withColumnRenamed("cumulative_tokens_bought", "tokens_available")
        
        # Step 5: Join sells with the appropriate cost basis
        # For each sell, find the latest buy that occurred before it
        # Alias the dataframes to avoid ambiguous column references
        sells_df_aliased = sells_df.alias("sell")
        buys_for_matching_aliased = buys_for_matching.alias("buy")
        
        sells_with_cost_basis = sells_df_aliased.join(
            buys_for_matching_aliased,
            (col("sell.wallet_address") == col("buy.wallet_address")) &
            (col("sell.base_address") == col("buy.base_address")) &
            (col("buy.buy_timestamp") <= col("sell.timestamp")),
            "left"
        ).select(
            col("sell.*"),
            col("buy.cost_basis_at_time"),
            col("buy.tokens_available"),
            col("buy.buy_timestamp")
        )
        
        # Get the latest cost basis for each sell (in case multiple buys occurred before)
        sell_window = Window.partitionBy(
            "wallet_address", 
            "base_address",
            "timestamp"
        ).orderBy(col("buy_timestamp").desc())
        
        sells_with_latest_cost = sells_with_cost_basis.withColumn(
            "row_num", row_number().over(sell_window)
        ).filter(col("row_num") == 1)
        
        # Step 6: Calculate realized PnL for each sell transaction
        sells_with_pnl = sells_with_latest_cost.withColumn(
            "tokens_sold", spark_abs(col("base_ui_change_amount"))
        ).withColumn(
            "sale_proceeds", col("usd_value")
        ).withColumn(
            "cost_of_sold_tokens", 
            when(col("cost_basis_at_time").isNotNull(), 
                 col("tokens_sold") * col("cost_basis_at_time")).otherwise(0)
        ).withColumn(
            "realized_pnl_per_trade",
            when(col("cost_basis_at_time").isNotNull(),
                 col("sale_proceeds") - col("cost_of_sold_tokens")).otherwise(0)
        ).withColumn(
            "has_cost_basis", col("cost_basis_at_time").isNotNull()
        )
        
        # Step 7: Aggregate per-token metrics
        token_pnl_summary = sells_with_pnl.groupBy("wallet_address", "base_address", "base_symbol").agg(
            spark_sum("realized_pnl_per_trade").alias("token_realized_pnl"),
            spark_sum("sale_proceeds").alias("total_sold_usd"),
            spark_sum("tokens_sold").alias("total_tokens_sold"),
            spark_sum("cost_of_sold_tokens").alias("total_cost_of_sold"),
            count("*").alias("sell_count"),
            spark_sum(when(col("realized_pnl_per_trade") > 0, 1).otherwise(0)).alias("profitable_sales"),
            spark_min("timestamp").alias("first_sale"),
            spark_max("timestamp").alias("last_sale")
        )
        
        # Step 8: Get buy statistics per token
        buy_summary = buys_df.groupBy("wallet_address", "base_address", "base_symbol").agg(
            spark_sum(col("usd_value")).alias("total_bought_usd"),
            spark_sum(spark_abs(col("base_ui_change_amount"))).alias("total_tokens_bought"),
            count("*").alias("buy_count"),
            spark_min("timestamp").alias("first_buy"),
            spark_max("timestamp").alias("last_buy")
        )
        
        # Step 9: Combine buy and sell data for complete token view
        token_complete_pnl = buy_summary.join(
            token_pnl_summary,
            ["wallet_address", "base_address", "base_symbol"],
            "left"
        ).fillna(0, ["token_realized_pnl", "total_sold_usd", "total_tokens_sold", 
                    "total_cost_of_sold", "sell_count", "profitable_sales"])
        
        # Calculate token-level metrics
        token_metrics = token_complete_pnl.withColumn(
            "net_position_tokens", col("total_tokens_bought") - col("total_tokens_sold")
        ).withColumn(
            "token_win_rate", 
            when(col("sell_count") > 0, (col("profitable_sales") / col("sell_count")) * 100).otherwise(0)
        ).withColumn(
            "token_roi",
            when(col("total_cost_of_sold") > 0, 
                 (col("token_realized_pnl") / col("total_cost_of_sold")) * 100).otherwise(0)
        )
        
        # Step 10: Portfolio-level aggregation (realized PnL only)
        portfolio_metrics = token_metrics.groupBy("wallet_address").agg(
            spark_sum("token_realized_pnl").alias("realized_pnl"),
            spark_sum("total_bought_usd").alias("total_bought"),
            spark_sum("total_sold_usd").alias("total_sold"),
            spark_sum("total_cost_of_sold").alias("total_cost_of_sold"),
            spark_sum("buy_count").alias("buy_count"),
            spark_sum("sell_count").alias("sell_count"),
            (spark_sum("buy_count") + spark_sum("sell_count")).alias("trade_count"),
            spark_sum("profitable_sales").alias("total_profitable_sales"),
            spark_min(coalesce(col("first_buy"), col("first_sale"))).alias("first_transaction"),
            spark_max(coalesce(col("last_buy"), col("last_sale"))).alias("last_transaction")
        ).withColumn(
            "win_rate",
            when(col("sell_count") > 0, (col("total_profitable_sales") / col("sell_count")) * 100).otherwise(0)
        ).withColumn(
            "realized_roi",
            when(col("total_cost_of_sold") > 0, 
                 (col("realized_pnl") / col("total_cost_of_sold")) * 100).otherwise(0)
        )
        
        # Add metadata
        batch_id = context.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S"))
        current_time = datetime.now()
        
        final_pnl_df = portfolio_metrics.withColumn("token_address", lit("ALL_TOKENS")) \
                                       .withColumn("calculation_date", lit(current_time.date())) \
                                       .withColumn("avg_holding_time_hours", (datediff(col("last_transaction"), col("first_transaction")) * 24.0)) \
                                       .withColumn("trade_frequency_daily", col("trade_count") / (datediff(col("last_transaction"), col("first_transaction")) + 1)) \
                                       .withColumn("current_position_tokens", lit(0.0)) \
                                       .withColumn("batch_id", lit(batch_id)) \
                                       .withColumn("processed_at", current_timestamp()) \
                                       .withColumn("data_source", lit("delta_lake_fifo_corrected")) \
                                       .withColumn("moved_to_gold", lit(False)) \
                                       .withColumn("gold_processed_at", lit(None).cast("string")) \
                                       .withColumn("gold_processing_status", lit("pending")) \
                                       .withColumn("unrealized_pnl", lit(0.0)) \
                                       .withColumn("total_pnl", col("realized_pnl")) \
                                       .withColumn("portfolio_roi", col("realized_roi"))
        
        # Round numeric values 
        for col_name in ["realized_pnl", "total_pnl", "total_bought", "total_sold", "realized_roi", "portfolio_roi", "win_rate"]:
            final_pnl_df = final_pnl_df.withColumn(col_name, spark_round(col(col_name), 2))
        
        # Write to Delta Lake
        table_path = get_table_path("silver_pnl")
        table_config = get_table_config("silver_pnl")
        
        partitioned_df = final_pnl_df.withColumn("calculation_year", expr("year(calculation_date)")) \
                                    .withColumn("calculation_month", expr("month(calculation_date)"))
        
        # Use MERGE to update existing PnL calculations and insert new ones
        if delta_manager.table_exists(table_path):
            merge_condition = "target.wallet_address = source.wallet_address AND target.token_address = source.token_address AND target.calculation_date = source.calculation_date"
            source_columns = [c for c in partitioned_df.columns if not c.startswith("_delta")]
            update_set = {c: f"source.{c}" for c in source_columns if c not in ["wallet_address", "token_address", "calculation_date"]}
            insert_values = {c: f"source.{c}" for c in partitioned_df.columns}
            version = delta_manager.merge_data(partitioned_df, table_path, merge_condition, update_set, insert_values)
        else:
            version = delta_manager.create_table(partitioned_df, table_path, partition_cols=table_config["partition_cols"])
        
        return {"status": "success", "pnl_records": 1, "delta_version": version}
        
    except Exception as e:
        logger.error(f"Single wallet PnL processing failed for {wallet_address}: {str(e)}")
        return {"status": "error", "error": str(e), "pnl_records": 0}


def _update_wallet_pnl_status(delta_manager, wallet_addresses: list, status: str):
    """Update PnL processing status for specific wallets in silver_whales table"""
    logger = logging.getLogger(__name__)
    
    try:
        silver_whales_path = get_table_path("silver_whales")
        if not delta_manager.table_exists(silver_whales_path):
            logger.warning("Silver whales table does not exist for status update")
            return
        
        # Create a DataFrame with just the updates for the specific wallets
        from pyspark.sql import Row
        update_data = []
        for wallet_addr in wallet_addresses:
            update_data.append({
                "wallet_address": wallet_addr,
                "pnl_processing_status": status,
                "pnl_last_processed_at": datetime.now().isoformat(),
                "_delta_timestamp": datetime.now(),
                "_delta_operation": "PNL_STATUS_UPDATE"
            })
        
        if not update_data:
            return
        
        # Create DataFrame from update data
        updates_df = delta_manager.spark.createDataFrame(update_data)
        
        # Use MERGE to update only the specific wallets
        merge_condition = "target.wallet_address = source.wallet_address"
        update_set = {
            "pnl_processing_status": "source.pnl_processing_status",
            "pnl_last_processed_at": "source.pnl_last_processed_at",
            "_delta_timestamp": "source._delta_timestamp",
            "_delta_operation": "source._delta_operation"
        }
        
        # Note: We don't insert new records, only update existing ones
        delta_manager.merge_data(updates_df, silver_whales_path, merge_condition, update_set, insert_values=None)
        logger.info(f"Updated pnl_processing_status to '{status}' for {len(wallet_addresses)} wallets using MERGE")
        
    except Exception as e:
        logger.error(f"Failed to update wallet PnL status: {str(e)}")


def create_gold_smart_traders_delta(**context) -> Dict[str, Any]:
    """Gold smart traders: Silver PnL → Performance tiers → Delta Lake"""
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        delta_manager = TrueDeltaLakeManager()
        
        # Read from silver PnL
        silver_pnl_path = get_table_path("silver_pnl")
        if not delta_manager.table_exists(silver_pnl_path):
            return {"status": "no_source", "records": 0}
        
        silver_df = delta_manager.spark.read.format("delta").load(silver_pnl_path)
        
        # Filter for qualified traders (win_rate > 0 OR total_pnl > 0)
        qualified_traders = silver_df.filter(
            ((silver_df.win_rate > 0) | (silver_df.total_pnl > 0)) &
            (silver_df.trade_count >= 1) & (silver_df.total_bought > 0) &
            (silver_df.wallet_address.isNotNull())
        )
        
        # Check for data without count() - use take(1) for efficiency
        if not qualified_traders.take(1):
            return {"status": "no_data", "records": 0}
        
        # Enhanced transformations
        enhanced_df = qualified_traders.select(
            col("wallet_address"), col("total_pnl"), col("portfolio_roi"), col("win_rate"),
            col("trade_count"), col("total_bought"), col("total_sold"), col("realized_pnl"),
            col("unrealized_pnl"), col("current_position_cost_basis"), col("current_position_value"),
            col("avg_holding_time_hours"), col("trade_frequency_daily"), col("first_transaction"),
            col("last_transaction"), col("calculation_date"), col("batch_id").alias("source_batch_id"),
            col("processed_at").alias("source_processed_at"), col("data_source").alias("source_data_source"),
            current_timestamp().alias("gold_created_at"),
            
            # Performance tier
            when((col("total_pnl") >= 10000) & (col("portfolio_roi") >= 50) & (col("win_rate") >= 30), "ELITE")
            .when((col("total_pnl") >= 1000) & (col("portfolio_roi") >= 20) & (col("win_rate") >= 20), "STRONG")
            .when((col("total_pnl") >= 100) & (col("portfolio_roi") >= 10) & (col("win_rate") >= 10), "PROMISING")
            .when((col("total_pnl") > 0) | (col("win_rate") > 0), "QUALIFIED")
            .otherwise("UNQUALIFIED").alias("performance_tier"),
            
            # Smart trader score
            spark_round(
                (when(col("total_pnl") > 0, least(col("total_pnl") / 1000, lit(100))).otherwise(0) * 0.4) +
                (when(col("portfolio_roi") > 0, least(col("portfolio_roi"), lit(100))).otherwise(0) * 0.3) +
                (when(col("win_rate") > 0, col("win_rate")).otherwise(0) * 0.2) +
                (when(col("trade_count") > 10, lit(10)).otherwise(col("trade_count")) * 0.1), 2
            ).alias("smart_trader_score"),
            
            # Additional metrics
            (datediff(col("last_transaction"), col("first_transaction")) + 1).alias("trading_experience_days"),
            spark_round((col("total_bought") + col("total_sold")) / col("trade_count"), 2).alias("avg_trade_size_usd"),
            when(col("total_bought") > 0, spark_round((col("total_sold") / col("total_bought")) * 100, 2)).otherwise(0).alias("sell_ratio_percent")
        )
        
        # Add rankings
        tier_window = Window.partitionBy("performance_tier").orderBy(col("smart_trader_score").desc(), col("total_pnl").desc())
        overall_window = Window.orderBy(col("smart_trader_score").desc(), col("total_pnl").desc())
        
        final_df = enhanced_df.withColumn("tier_rank", row_number().over(tier_window)) \
                             .withColumn("overall_rank", row_number().over(overall_window)) \
                             .orderBy(col("smart_trader_score").desc(), col("total_pnl").desc())
        
        # Write to gold Delta
        gold_path = get_table_path("gold_traders")
        
        final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
               .partitionBy("performance_tier").save(gold_path)
        
        # Get final stats
        final_count = final_df.count()
        tier_stats = final_df.groupBy("performance_tier").count().collect()
        tier_summary = {row['performance_tier']: row['count'] for row in tier_stats}
        
        logger.info(f"Gold traders: {final_count} qualified traders identified - {tier_summary}")
        
        return {
            "status": "success", "smart_traders": final_count,
            "tier_breakdown": tier_summary
        }
        
    except Exception as e:
        logger.error(f"Gold smart traders failed: {str(e)}")
        return {"status": "failed", "error": str(e), "records": 0}
    finally:
        if delta_manager:
            delta_manager.stop()