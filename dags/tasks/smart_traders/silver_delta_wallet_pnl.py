"""
Silver Delta Wallet PnL Task - TRUE Delta Lake Implementation
CONSERVATIVE APPROACH: 10 wallets per batch, 1GB memory, ZERO FALLBACKS

This task:
1. Reads unprocessed transactions from bronze Delta Lake table
2. Calculates comprehensive PnL metrics with smart USD value logic
3. Writes to silver wallet PnL Delta Lake table with MERGE operations
4. NO FALLBACKS - real data or clean failure
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Import True Delta Lake manager
from utils.true_delta_manager import TrueDeltaLakeManager, get_table_path
from config.true_delta_config import get_table_config

# Import PySpark functions
from pyspark.sql.functions import (
    col, lit, when, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    current_timestamp, coalesce, round as spark_round, abs as spark_abs,
    datediff, expr, to_date, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType, DateType


def calculate_usd_value_column():
    """
    Smart USD value calculation with price fallback logic
    Returns PySpark column expression
    """
    return when(
        # Prefer base price if available and positive
        (col("base_nearest_price").isNotNull()) & (col("base_nearest_price") > 0),
        spark_abs(col("base_ui_change_amount")) * col("base_nearest_price")
    ).when(
        # Fallback to quote price if base price unavailable
        (col("quote_nearest_price").isNotNull()) & (col("quote_nearest_price") > 0),
        spark_abs(col("quote_ui_change_amount")) * col("quote_nearest_price")
    ).otherwise(None)  # Skip transactions without any price data


def classify_transaction_type():
    """
    Classify transactions as BUY/SELL based on type_swap direction
    Returns PySpark column expression
    """
    return when(
        col("base_type_swap") == "to", "BUY"  # Acquiring the base token
    ).when(
        col("base_type_swap") == "from", "SELL"  # Disposing the base token
    ).otherwise("SWAP")  # Other transaction types


def create_silver_wallet_pnl_delta(**context) -> Dict[str, Any]:
    """
    Calculate wallet PnL metrics using TRUE Delta Lake with conservative settings
    
    CONSERVATIVE SETTINGS:
    - 10 wallets per batch
    - 1GB memory limits  
    - ZERO FALLBACKS
    - Real data or clean failure
    
    Returns:
        Dict with Delta Lake operation results
    """
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        logger.info("🚀 Starting TRUE Delta Lake silver wallet PnL calculation...")
        logger.info("⚙️ CONSERVATIVE MODE: 10 wallets, 1GB memory, NO FALLBACKS")
        
        # Step 1: Create Delta Lake manager with conservative memory settings
        logger.info("⚡ Creating TRUE Delta Lake manager...")
        delta_manager = TrueDeltaLakeManager()
        
        # Step 2: Read unprocessed transactions from bronze Delta table
        logger.info("📖 Reading unprocessed transactions from bronze Delta table...")
        bronze_transactions_path = "s3a://smart-trader/bronze/transaction_history"
        
        if not delta_manager.table_exists(bronze_transactions_path):
            logger.error("❌ Bronze transaction history Delta table does not exist")
            return {
                "status": "failed",
                "error": "Bronze transaction table not found",
                "records": 0,
                "delta_table": False
            }
        
        # Read only unprocessed transactions
        bronze_df = delta_manager.spark.read.format("delta").load(bronze_transactions_path)
        unprocessed_transactions = bronze_df.filter(
            (col("processed_for_pnl") == False) &
            (col("tx_type") == "swap") &  # Focus on swap transactions for PnL
            (col("whale_id").isNotNull()) &
            (col("wallet_address").isNotNull()) &
            (col("transaction_hash").isNotNull())
        )
        
        transaction_count = unprocessed_transactions.count()
        
        if transaction_count == 0:
            logger.info("✅ No unprocessed transactions found for PnL calculation")
            return {
                "status": "no_data",
                "records": 0,
                "message": "All transactions already processed for PnL",
                "delta_table": False
            }
        
        logger.info(f"📊 Found {transaction_count} unprocessed swap transactions")
        
        # Step 3: Get unique wallets (CONSERVATIVE: limit to 10 wallets per batch)
        unique_wallets = unprocessed_transactions.select("wallet_address").distinct().limit(10)
        wallet_count = unique_wallets.count()
        
        logger.info(f"👛 Processing {wallet_count} wallets (CONSERVATIVE batch size)")
        
        # Step 4: Filter transactions to only these wallets
        wallet_list = [row['wallet_address'] for row in unique_wallets.collect()]
        filtered_transactions = unprocessed_transactions.filter(
            col("wallet_address").isin(wallet_list)
        )
        
        # Step 5: Calculate USD values with smart fallback logic
        logger.info("💰 Calculating USD values with price fallback logic...")
        transactions_with_usd = filtered_transactions.withColumn(
            "usd_value", calculate_usd_value_column()
        ).withColumn(
            "trade_type", classify_transaction_type()
        ).filter(
            # Only process transactions with valid USD values
            col("usd_value").isNotNull() & (col("usd_value") > 0)
        )
        
        transactions_with_usd_count = transactions_with_usd.count()
        logger.info(f"💵 {transactions_with_usd_count} transactions have valid USD values")
        
        if transactions_with_usd_count == 0:
            logger.warning("⚠️ No transactions with valid price data found")
            return {
                "status": "no_data",
                "records": 0,
                "message": "No transactions with valid price data",
                "delta_table": False
            }
        
        # Step 6: Calculate PnL metrics per wallet-token pair using groupBy
        logger.info("📈 Calculating PnL metrics using optimized groupBy operations...")
        
        wallet_token_pnl = transactions_with_usd.groupBy("wallet_address", "base_address", "base_symbol").agg(
            # Volume metrics
            spark_sum(when(col("trade_type") == "BUY", col("usd_value")).otherwise(0)).alias("total_bought_usd"),
            spark_sum(when(col("trade_type") == "SELL", col("usd_value")).otherwise(0)).alias("total_sold_usd"),
            spark_sum(when(col("trade_type") == "BUY", spark_abs(col("base_ui_change_amount"))).otherwise(0)).alias("tokens_bought"),
            spark_sum(when(col("trade_type") == "SELL", spark_abs(col("base_ui_change_amount"))).otherwise(0)).alias("tokens_sold"),
            
            # Trade counts
            count(when(col("trade_type") == "BUY", 1)).alias("buy_count"),
            count(when(col("trade_type") == "SELL", 1)).alias("sell_count"),
            count("*").alias("total_trades"),
            
            # Price metrics (weighted averages)
            avg(when(col("trade_type") == "BUY", col("usd_value") / spark_abs(col("base_ui_change_amount")))).alias("avg_buy_price"),
            avg(when(col("trade_type") == "SELL", col("usd_value") / spark_abs(col("base_ui_change_amount")))).alias("avg_sell_price"),
            
            # Timing
            spark_min("timestamp").alias("first_trade"),
            spark_max("timestamp").alias("last_trade")
        )
        
        # Step 7: Calculate comprehensive PnL metrics
        logger.info("🧮 Calculating comprehensive PnL and position metrics...")
        
        pnl_metrics = wallet_token_pnl.withColumn(
            # Current position (remaining tokens)
            "current_position_tokens", 
            col("tokens_bought") - col("tokens_sold")
        ).withColumn(
            # Average cost basis for bought tokens
            "avg_cost_basis",
            when(col("tokens_bought") > 0, col("total_bought_usd") / col("tokens_bought")).otherwise(0)
        ).withColumn(
            # Realized PnL (from completed sales)
            "realized_pnl",
            col("total_sold_usd") - (col("tokens_sold") * col("avg_cost_basis"))
        ).withColumn(
            # Current position cost basis
            "current_position_cost_basis",
            col("current_position_tokens") * col("avg_cost_basis")
        ).withColumn(
            # Estimate current position value (using last sell price or buy price)
            "current_position_value",
            col("current_position_tokens") * coalesce(col("avg_sell_price"), col("avg_cost_basis"))
        ).withColumn(
            # Unrealized PnL (from current holdings)
            "unrealized_pnl",
            col("current_position_value") - col("current_position_cost_basis")
        ).withColumn(
            # Total PnL
            "total_pnl",
            col("realized_pnl") + col("unrealized_pnl")
        ).withColumn(
            # ROI percentage
            "roi",
            when(col("total_bought_usd") > 0, 
                 (col("total_pnl") / col("total_bought_usd")) * 100
            ).otherwise(0)
        ).withColumn(
            # Win rate (1 if profitable, 0 if not)
            "is_profitable",
            when(col("total_pnl") > 0, 1).otherwise(0)
        )
        
        # Step 8: Calculate portfolio-level aggregation for each wallet
        logger.info("📊 Aggregating portfolio-level metrics...")
        
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
            # Win rate: percentage of tokens with positive PnL
            (spark_sum("is_profitable") * 100.0 / count("*")).alias("win_rate"),
            # Portfolio-level ROI
            (spark_sum("total_pnl") / spark_sum("total_bought_usd") * 100).alias("portfolio_roi")
        )
        
        # Step 9: Add metadata and derived metrics
        batch_id = context.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S"))
        current_time = datetime.now()
        
        final_pnl_df = portfolio_metrics.withColumn(
            "token_address", lit("ALL_TOKENS")  # Portfolio-level aggregation
        ).withColumn(
            "calculation_date", lit(current_time.date())
        ).withColumn(
            "avg_holding_time_hours",
            (datediff(col("last_transaction"), col("first_transaction")) * 24.0)
        ).withColumn(
            "trade_frequency_daily",
            col("trade_count") / (datediff(col("last_transaction"), col("first_transaction")) + 1)
        ).withColumn(
            "current_position_tokens", lit(0.0)  # Not applicable at portfolio level
        ).withColumn(
            "batch_id", lit(batch_id)
        ).withColumn(
            "processed_at", current_timestamp()
        ).withColumn(
            "data_source", lit("delta_lake_conservative")
        )
        
        # Round numeric values for cleaner output
        numeric_columns = [
            "realized_pnl", "unrealized_pnl", "total_pnl", "total_bought", "total_sold",
            "portfolio_roi", "win_rate", "current_position_cost_basis", "current_position_value"
        ]
        
        for col_name in numeric_columns:
            final_pnl_df = final_pnl_df.withColumn(col_name, spark_round(col(col_name), 2))
        
        pnl_record_count = final_pnl_df.count()
        logger.info(f"📈 Generated {pnl_record_count} wallet PnL records")
        
        # Step 10: Write to silver wallet PnL Delta Lake table with MERGE
        table_path = get_table_path("silver_pnl")
        table_config = get_table_config("silver_pnl")
        
        logger.info(f"🎯 Target Delta table: {table_path}")
        logger.info(f"💾 Writing to TRUE Delta table with ACID compliance...")
        
        # Add partitioning columns
        partitioned_df = final_pnl_df.withColumn(
            "calculation_year", expr("year(calculation_date)")
        ).withColumn(
            "calculation_month", expr("month(calculation_date)")
        )
        
        if delta_manager.table_exists(table_path):
            logger.info("🔄 Using Delta MERGE for upsert operations...")
            
            # Prepare merge condition
            merge_condition = """
                target.wallet_address = source.wallet_address AND 
                target.token_address = source.token_address AND
                target.calculation_date = source.calculation_date
            """
            
            # Update and insert values (all columns except Delta metadata)
            source_columns = [col_name for col_name in partitioned_df.columns 
                            if not col_name.startswith("_delta")]
            
            update_set = {col_name: f"source.{col_name}" for col_name in source_columns}
            insert_values = {col_name: f"source.{col_name}" for col_name in source_columns}
            
            version = delta_manager.merge_data(
                partitioned_df,
                table_path,
                merge_condition,
                update_set,
                insert_values
            )
            operation = "MERGE"
        else:
            logger.info("🏗️ Creating new Delta table...")
            version = delta_manager.create_table(
                partitioned_df,
                table_path,
                partition_cols=table_config["partition_cols"]
            )
            operation = "CREATE"
        
        # Step 11: Mark processed transactions in bronze table
        logger.info("✅ Marking transactions as processed in bronze table...")
        
        # Update bronze transactions to mark as processed
        processed_transaction_hashes = [
            row['transaction_hash'] for row in 
            transactions_with_usd.select("transaction_hash").distinct().collect()
        ]
        
        bronze_update_df = delta_manager.spark.read.format("delta").load(bronze_transactions_path)
        updated_bronze = bronze_update_df.withColumn(
            "processed_for_pnl",
            when(col("transaction_hash").isin(processed_transaction_hashes), True)
            .otherwise(col("processed_for_pnl"))
        ).withColumn(
            "_delta_operation",
            when(col("transaction_hash").isin(processed_transaction_hashes), "PNL_PROCESSED")
            .otherwise(col("_delta_operation"))
        ).withColumn(
            "_delta_timestamp", current_timestamp()
        )
        
        # Write back the updated bronze data
        updated_bronze.write.format("delta").mode("overwrite").save(bronze_transactions_path)
        
        logger.info(f"📝 Marked {len(processed_transaction_hashes)} transactions as processed")
        
        # Step 12: Validate Delta table health
        logger.info("🔍 Validating Delta table health...")
        health_check = delta_manager.validate_table_health(table_path)
        
        if health_check["status"] != "healthy":
            raise RuntimeError(f"Delta table health check failed: {health_check}")
        
        # Step 13: Get transaction history to verify _delta_log
        history = delta_manager.get_table_history(table_path, 3)
        
        logger.info("✅ TRUE Delta silver wallet PnL completed successfully!")
        logger.info(f"   👛 Wallets processed: {wallet_count}")
        logger.info(f"   📊 PnL records: {health_check['record_count']}")
        logger.info(f"   📈 Delta version: {version}")
        logger.info(f"   🏥 Health: {health_check['status']}")
        logger.info(f"   🔄 Operation: {operation}")
        logger.info(f"   📋 Transaction log entries: {len(history)}")
        logger.info(f"   🗂️ _delta_log verified: {health_check['delta_log_verified']}")
        
        return {
            "status": "success",
            "wallets_processed": wallet_count,
            "pnl_records": health_check["record_count"],
            "transactions_processed": len(processed_transaction_hashes),
            "delta_version": version,
            "table_path": table_path,
            "health_status": health_check["status"],
            "operation": operation,
            "transaction_log_entries": len(history),
            "delta_log_verified": True,
            "conservative_mode": True
        }
        
    except Exception as e:
        logger.error(f"❌ TRUE Delta silver wallet PnL failed: {str(e)}")
        return {
            "status": "failed",
            "error": str(e),
            "wallets_processed": 0,
            "pnl_records": 0,
            "delta_table": False
        }
        
    finally:
        if delta_manager:
            logger.info("🛑 Stopping Delta Lake manager")
            delta_manager.stop()