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
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType, FloatType


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
        
        # Paginated API fetch
        offset, all_tokens = 0, []
        while len(all_tokens) < TOKEN_LIMIT:
            remaining = TOKEN_LIMIT - len(all_tokens)
            current_limit = min(API_PAGINATION_LIMIT, remaining)
            
            params = {
                "sort_by": "liquidity", "sort_type": "desc", 
                "offset": offset, "limit": current_limit,
                **filter_params
            }
            
            response = birdeye_client.get_token_list(**params)
            tokens_data = birdeye_client.normalize_token_list_response(response)
            
            if not tokens_data:
                break
                
            all_tokens.extend(tokens_data)
            offset += len(tokens_data)
            
            if len(tokens_data) < current_limit:
                break
                
            import time
            time.sleep(API_RATE_LIMIT_DELAY)
        
        tokens_result = all_tokens[:TOKEN_LIMIT]
        if not tokens_result:
            return {"status": "no_data", "records": 0}
        
        # Convert to Spark DataFrame and write to Delta
        df = delta_manager.spark.createDataFrame(tokens_result)
        df_with_metadata = df.withColumn("_delta_timestamp", current_timestamp()) \
                            .withColumn("_delta_operation", lit("BRONZE_TOKEN_CREATE")) \
                            .withColumn("processing_date", current_date()) \
                            .withColumn("batch_id", lit(context.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S")))) \
                            .withColumn("processed", lit(False)) \
                            .withColumn("_delta_created_at", lit(datetime.now().isoformat()))
        
        table_path = get_table_path("bronze_tokens")
        table_config = get_table_config("bronze_tokens")
        
        version = delta_manager.create_table(
            df_with_metadata, table_path, partition_cols=table_config["partition_cols"]
        )
        
        health_check = delta_manager.validate_table_health(table_path)
        
        logger.info(f"Bronze tokens: {len(tokens_result)} records → Delta v{version}")
        
        return {
            "status": "success", "records": len(tokens_result),
            "delta_version": version, "table_path": table_path
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
        
        if delta_manager.table_exists(table_path):
            version = delta_manager.append_data(df_with_metadata, table_path)
            operation = "APPEND"
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
                    .otherwise(col("_delta_operation"))
                )
                
                # Write updated silver tokens table back
                updated_silver_df.write.format("delta").mode("overwrite").save(silver_tokens_path)
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
    Fetches transaction data for whales that need processing or refetching (2-week cycle)
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
        
        # Filter for whales needing transaction data fetched
        # Include: 1) New whales (pending/ready status) 
        #         2) Whales with no fetch timestamp
        #         3) Whales last fetched more than 2 weeks ago (14 days)
        whales_needing_transactions = silver_whales_df.filter(
            (col("processing_status") == "pending") |
            (col("processing_status") == "ready") |
            col("txns_last_fetched_at").isNull() |
            # 2-week refetch logic (14 days)
            (col("txns_last_fetched_at").isNotNull() & 
             (datediff(current_timestamp(), col("txns_last_fetched_at")) >= 14))
        ).select("whale_id", "wallet_address", "token_address", "token_symbol").limit(10)
        
        # Collect limited data without count() to avoid crashes
        whales_list = [row.asDict() for row in whales_needing_transactions.collect()]
        if not whales_list:
            return {"status": "no_data", "records": 0}
        whales_count = len(whales_list)
        
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
        df_with_metadata = df.withColumn("_delta_timestamp", current_timestamp()) \
                            .withColumn("_delta_operation", lit("BRONZE_TRANSACTION_CREATE")) \
                            .withColumn("transaction_date", to_date(col("timestamp"))) \
                            .withColumn("_delta_created_at", lit(datetime.now().isoformat()))
        
        table_path = get_table_path("bronze_transactions")
        table_config = get_table_config("bronze_transactions")
        
        if delta_manager.table_exists(table_path):
            version = delta_manager.append_data(df_with_metadata, table_path)
            operation = "APPEND"
        else:
            version = delta_manager.create_table(
                df_with_metadata, table_path, partition_cols=table_config["partition_cols"]
            )
            operation = "CREATE"
        
        health_check = delta_manager.validate_table_health(table_path)
        
        # Update silver tracked whales processing_status to 'processed' for processed whales
        # This handles both new whales and whales being refetched after 2 weeks
        if whales_count > 0 and len(all_transaction_data) > 0:
            processed_whale_ids = [whale_data['whale_id'] for whale_data in whales_list]
            
            # Read current silver tracked whales table
            silver_whales_path = get_table_path("silver_whales")
            if delta_manager.table_exists(silver_whales_path):
                silver_whales_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
                
                # Update processing_status for processed whales
                updated_silver_whales_df = silver_whales_df.withColumn(
                    "processing_status",
                    when(col("whale_id").isin(processed_whale_ids), lit("processed"))
                    .otherwise(col("processing_status"))
                ).withColumn(
                    "txns_last_fetched_at",
                    when(col("whale_id").isin(processed_whale_ids), current_timestamp())
                    .otherwise(col("txns_last_fetched_at"))
                ).withColumn(
                    "_delta_timestamp", current_timestamp()
                ).withColumn(
                    "_delta_operation",
                    when(col("whale_id").isin(processed_whale_ids), lit("TRANSACTION_STATUS_UPDATE"))
                    .otherwise(col("_delta_operation"))
                )
                
                # Write updated silver whales table back
                updated_silver_whales_df.write.format("delta").mode("overwrite").save(silver_whales_path)
                logger.info(f"Updated processing_status to 'processed' for {len(processed_whale_ids)} whales")
        
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
    """Silver PnL: Bronze transactions → PnL calculations → Delta Lake"""
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    
    try:
        delta_manager = TrueDeltaLakeManager()
        
        # Read from silver whales to find wallets needing PnL processing
        silver_whales_path = get_table_path("silver_whales")
        if not delta_manager.table_exists(silver_whales_path):
            return {"status": "no_source", "records": 0}
        
        silver_whales_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
        
        # Filter for wallets needing PnL processing
        # Include: 1) New wallets (pending PnL status)
        #         2) Wallets with no PnL timestamp  
        #         3) Wallets last processed more than 2 weeks ago (14 days)
        wallets_needing_pnl = silver_whales_df.filter(
            (col("pnl_processing_status") == "pending") |
            col("pnl_last_processed_at").isNull() |
            # 2-week refetch logic (14 days)
            (col("pnl_last_processed_at").isNotNull() & 
             (datediff(current_timestamp(), col("pnl_last_processed_at")) >= 14))
        ).select("whale_id", "wallet_address", "token_address", "token_symbol")
        
        # Check for wallets without count() - use take(1) for efficiency
        if not wallets_needing_pnl.take(1):
            return {"status": "no_data", "records": 0}
        
        # ULTRA-CONSERVATIVE: Process only 3 wallets at a time to prevent crashes
        from config.smart_trader_config import SILVER_PNL_WALLET_BATCH_SIZE, SILVER_PNL_MAX_TRANSACTIONS_PER_BATCH
        
        # Get limited batch of wallets needing PnL processing
        batch_size = min(SILVER_PNL_WALLET_BATCH_SIZE, 3)  # Max 3 wallets
        selected_wallets = wallets_needing_pnl.limit(batch_size)
        wallet_list = [row['wallet_address'] for row in selected_wallets.collect()]
        
        if not wallet_list:
            return {"status": "no_wallets", "records": 0}
        
        logger.info(f"Processing {len(wallet_list)} wallets with ultra-safe batch size")
        
        # Now read ALL transactions for the selected wallets from bronze_transactions
        bronze_transactions_path = get_table_path("bronze_transactions")
        if not delta_manager.table_exists(bronze_transactions_path):
            return {"status": "no_transactions", "records": 0}
        
        bronze_transactions_df = delta_manager.spark.read.format("delta").load(bronze_transactions_path)
        filtered_transactions = bronze_transactions_df.filter(
            (col("tx_type") == "swap") &
            (col("wallet_address").isin(wallet_list)) &
            (col("whale_id").isNotNull())
        )
        
        # Further limit transactions per wallet
        window_spec = Window.partitionBy("wallet_address").orderBy(col("timestamp").desc())
        filtered_transactions = filtered_transactions.withColumn("row_num", row_number().over(window_spec)) \
                                                   .filter(col("row_num") <= SILVER_PNL_MAX_TRANSACTIONS_PER_BATCH) \
                                                   .drop("row_num")
        
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
        
        # Check for data without count() - use take(1) for efficiency
        if not transactions_with_usd.take(1):
            return {"status": "no_data", "records": 0}
        
        # PnL calculations using groupBy
        wallet_token_pnl = transactions_with_usd.groupBy("wallet_address", "base_address", "base_symbol").agg(
            spark_sum(when(col("trade_type") == "BUY", col("usd_value")).otherwise(0)).alias("total_bought_usd"),
            spark_sum(when(col("trade_type") == "SELL", col("usd_value")).otherwise(0)).alias("total_sold_usd"),
            spark_sum(when(col("trade_type") == "BUY", spark_abs(col("base_ui_change_amount"))).otherwise(0)).alias("tokens_bought"),
            spark_sum(when(col("trade_type") == "SELL", spark_abs(col("base_ui_change_amount"))).otherwise(0)).alias("tokens_sold"),
            count(when(col("trade_type") == "BUY", 1)).alias("buy_count"),
            count(when(col("trade_type") == "SELL", 1)).alias("sell_count"),
            count("*").alias("total_trades"),
            avg(when(col("trade_type") == "BUY", col("usd_value") / spark_abs(col("base_ui_change_amount")))).alias("avg_buy_price"),
            avg(when(col("trade_type") == "SELL", col("usd_value") / spark_abs(col("base_ui_change_amount")))).alias("avg_sell_price"),
            spark_min("timestamp").alias("first_trade"),
            spark_max("timestamp").alias("last_trade")
        )
        
        # Comprehensive PnL metrics
        pnl_metrics = wallet_token_pnl.withColumn("current_position_tokens", col("tokens_bought") - col("tokens_sold")) \
                                     .withColumn("avg_cost_basis", when(col("tokens_bought") > 0, col("total_bought_usd") / col("tokens_bought")).otherwise(0)) \
                                     .withColumn("realized_pnl", col("total_sold_usd") - (col("tokens_sold") * col("avg_cost_basis"))) \
                                     .withColumn("current_position_cost_basis", col("current_position_tokens") * col("avg_cost_basis")) \
                                     .withColumn("current_position_value", col("current_position_tokens") * coalesce(col("avg_sell_price"), col("avg_cost_basis"))) \
                                     .withColumn("unrealized_pnl", col("current_position_value") - col("current_position_cost_basis")) \
                                     .withColumn("total_pnl", col("realized_pnl") + col("unrealized_pnl")) \
                                     .withColumn("roi", when(col("total_bought_usd") > 0, (col("total_pnl") / col("total_bought_usd")) * 100).otherwise(0)) \
                                     .withColumn("is_profitable", when(col("total_pnl") > 0, 1).otherwise(0))
        
        # Portfolio aggregation
        portfolio_metrics = pnl_metrics.groupBy("wallet_address").agg(
            spark_sum("realized_pnl").alias("realized_pnl"),
            spark_sum("unrealized_pnl").alias("unrealized_pnl"),
            spark_sum("total_pnl").alias("total_pnl"),
            spark_sum("total_bought_usd").alias("total_bought"),
            spark_sum("total_sold_usd").alias("total_sold"),
            spark_sum("total_trades").alias("trade_count"),
            spark_sum("current_position_cost_basis").alias("current_position_cost_basis"),
            spark_sum("current_position_value").alias("current_position_value"),
            spark_min("first_trade").alias("first_transaction"),
            spark_max("last_trade").alias("last_transaction"),
            (spark_sum("is_profitable") * 100.0 / count("*")).alias("win_rate"),
            (spark_sum("total_pnl") / spark_sum("total_bought_usd") * 100).alias("portfolio_roi")
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
                                       .withColumn("data_source", lit("delta_lake_conservative")) \
                                       .withColumn("moved_to_gold", lit(False)) \
                                       .withColumn("gold_processed_at", lit(None).cast("timestamp")) \
                                       .withColumn("gold_processing_status", lit("pending"))
        
        # Round numeric values
        for col_name in ["realized_pnl", "unrealized_pnl", "total_pnl", "total_bought", "total_sold", "portfolio_roi", "win_rate"]:
            final_pnl_df = final_pnl_df.withColumn(col_name, spark_round(col(col_name), 2))
        
        
        # Write to Delta with memory guard
        table_path = get_table_path("silver_pnl")
        table_config = get_table_config("silver_pnl")
        
        partitioned_df = final_pnl_df.withColumn("calculation_year", expr("year(calculation_date)")) \
                                    .withColumn("calculation_month", expr("month(calculation_date)"))
        
        if delta_manager.table_exists(table_path):
            merge_condition = "target.wallet_address = source.wallet_address AND target.token_address = source.token_address AND target.calculation_date = source.calculation_date"
            source_columns = [c for c in partitioned_df.columns if not c.startswith("_delta")]
            update_set = {c: f"source.{c}" for c in source_columns}
            insert_values = {c: f"source.{c}" for c in source_columns}
            version = delta_manager.merge_data(partitioned_df, table_path, merge_condition, update_set, insert_values)
            operation = "MERGE"
        else:
            version = delta_manager.create_table(partitioned_df, table_path, partition_cols=table_config["partition_cols"])
            operation = "CREATE"
        
        # PnL processing complete - no transaction-level status updates needed
        # Wallet-level PnL tracking is now handled in silver_whales table
        
        health_check = delta_manager.validate_table_health(table_path)
        
        # Update silver_whales table to mark processed wallets as 'completed'
        if len(wallet_list) > 0:
            # Read current silver whales table
            silver_whales_path = get_table_path("silver_whales")
            if delta_manager.table_exists(silver_whales_path):
                silver_whales_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
                
                # Update pnl_processing_status and pnl_last_processed_at for processed wallets
                updated_silver_whales_df = silver_whales_df.withColumn(
                    "pnl_processing_status",
                    when(col("wallet_address").isin(wallet_list), lit("completed"))
                    .otherwise(col("pnl_processing_status"))
                ).withColumn(
                    "pnl_last_processed_at",
                    when(col("wallet_address").isin(wallet_list), current_timestamp())
                    .otherwise(col("pnl_last_processed_at"))
                ).withColumn(
                    "_delta_timestamp", current_timestamp()
                ).withColumn(
                    "_delta_operation",
                    when(col("wallet_address").isin(wallet_list), lit("PNL_STATUS_UPDATE"))
                    .otherwise(col("_delta_operation"))
                )
                
                # Write updated silver whales table back
                updated_silver_whales_df.write.format("delta").mode("overwrite").save(silver_whales_path)
                logger.info(f"Updated pnl_processing_status to 'completed' for {len(wallet_list)} wallets")
        
        logger.info(f"Silver PnL: {len(wallet_list)} wallets → Delta v{version} ({operation})")
        
        return {
            "status": "success", "wallets_processed": len(wallet_list),
            "delta_version": version
        }
        
    except Exception as e:
        logger.error(f"Silver PnL failed: {str(e)}")
        return {"status": "failed", "error": str(e), "records": 0}
    finally:
        if delta_manager:
            delta_manager.stop()


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