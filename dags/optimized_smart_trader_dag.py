"""
Optimized Smart Trader Identification DAG - TRUE Delta Lake Implementation
Uses only Delta Lake tasks with ACID transactions and proper state tracking
"""
import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Import centralized configuration
from config.smart_trader_config import (
    # DAG Configuration
    DAG_SCHEDULE_INTERVAL, DAG_MAX_ACTIVE_RUNS, DAG_CATCHUP, DAG_RETRIES,
    DAG_RETRY_DELAY_MINUTES, DAG_START_DAYS_AGO, DAG_OWNER, DAG_DEPENDS_ON_PAST,
    DAG_EMAIL_ON_FAILURE, DAG_EMAIL_ON_RETRY, DAG_TAGS,
    # API Configuration
    API_RATE_LIMIT_CODES, API_AUTH_ERROR_CODES, API_TIMEOUT_KEYWORDS,
    API_RATE_LIMIT_KEYWORDS, API_AUTH_KEYWORDS,
    # Processing Configuration
    DATA_EMPTY_KEYWORDS, STORAGE_ERROR_KEYWORDS, DBT_ERROR_KEYWORDS
)

@task
def fetch_bronze_tokens(**context):
    """Fetch token list to smart-trader bucket using BirdEye API with TRUE Delta Lake"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.optimized_delta_tasks import create_bronze_tokens_delta
        result = create_bronze_tokens_delta(**context)
        
        if result and isinstance(result, dict):
            tokens_count = result.get('tokens_processed', 0)
            logger.info(f"✅ Bronze tokens: {tokens_count} processed with Delta Lake")
            return {"status": "success", "tokens_count": tokens_count}
        else:
            logger.warning("⚠️ No token data returned")
            return {"status": "no_data", "tokens_count": 0}
            
    except Exception as e:
        if any(code in str(e) for code in API_RATE_LIMIT_CODES):
            logger.error("❌ BirdEye API rate limit exceeded")
        elif any(code in str(e) for code in API_AUTH_ERROR_CODES):
            logger.error("❌ BirdEye API authentication failed")
        else:
            logger.error(f"❌ Bronze tokens failed: {str(e)}")
        raise

@task
def create_silver_tracked_tokens(**context):
    """Create silver tracked tokens by filtering bronze tokens by liquidity using Delta Lake"""
    logger = logging.getLogger(__name__)
    
    try:
        # Memory safety check
        import psutil
        available_memory = psutil.virtual_memory().available / (1024**3)  # GB
        if available_memory < 2.0:
            logger.warning(f"Low memory: {available_memory:.1f}GB available, proceeding with minimal processing")
            # Could return early or use smaller batch sizes
        from utils.true_delta_manager import TrueDeltaLakeManager
        from pyspark.sql.functions import col, lit, current_timestamp, row_number, when, coalesce
        from pyspark.sql.window import Window
        
        delta_manager = TrueDeltaLakeManager()
        
        # Read bronze tokens from Delta Lake
        bronze_tokens_path = "s3a://smart-trader/bronze/token_metrics"
        if not delta_manager.table_exists(bronze_tokens_path):
            logger.warning("Bronze tokens table not found")
            return {"status": "no_source", "records": 0}
        
        bronze_df = delta_manager.spark.read.format("delta").load(bronze_tokens_path)
        
        # Apply silver layer transformations (similar to dbt model)
        filtered_df = bronze_df.filter(
            (col("liquidity") >= 100000) &  # Min $100K liquidity
            col("token_address").isNotNull() &
            col("symbol").isNotNull() &
            col("liquidity").isNotNull() &
            col("price").isNotNull()
        )
        
        # Add liquidity tier and other silver layer fields
        window_spec = Window.partitionBy("token_address").orderBy(col("processing_date").desc(), col("_delta_timestamp").desc())
        
        silver_df = filtered_df.withColumn(
            "liquidity_tier",
            when(col("liquidity") >= 1000000, "HIGH")
            .when(col("liquidity") >= 500000, "MEDIUM")
            .when(col("liquidity") >= 100000, "LOW")
            .otherwise("MINIMAL")
        ).withColumn(
            "fdv_per_holder",
            when(col("holder") > 0, col("fdv") / col("holder")).otherwise(None)
        ).withColumn(
            "whale_fetch_status", lit("pending")
        ).withColumn(
            "whale_fetched_at", lit(None).cast("timestamp")
        ).withColumn(
            "is_newly_tracked", lit(True)
        ).withColumn(
            "silver_created_at", current_timestamp()
        ).withColumn(
            "rn", row_number().over(window_spec)
        ).filter(col("rn") == 1).drop("rn")
        
        # Write to silver tracked tokens Delta table
        silver_tokens_path = "s3a://smart-trader/silver/tracked_tokens_delta"
        version = delta_manager.create_table(
            silver_df, 
            silver_tokens_path,
            partition_cols=None,  # No partitioning for silver layer
            merge_schema=True
        )
        
        record_count = silver_df.count()
        logger.info(f"✅ Silver tracked tokens: {record_count} records created in Delta v{version}")
        
        return {
            "status": "success", 
            "records": record_count,
            "delta_version": version,
            "table_path": silver_tokens_path
        }
        
    except Exception as e:
        logger.error(f"❌ Silver tracked tokens failed: {str(e)}")
        raise
    finally:
        if 'delta_manager' in locals():
            delta_manager.stop()

@task
def create_silver_tracked_whales(**context):
    """Create silver tracked whales by filtering and enhancing bronze whale holders using Delta Lake"""
    logger = logging.getLogger(__name__)
    
    try:
        from utils.true_delta_manager import TrueDeltaLakeManager
        from pyspark.sql.functions import col, lit, current_timestamp, row_number, when, concat, coalesce
        from pyspark.sql.window import Window
        
        delta_manager = TrueDeltaLakeManager()
        
        # Read bronze whale holders from Delta Lake (now should exist after bronze_whales task)
        bronze_whales_path = "s3a://smart-trader/bronze/whale_holders"
        if not delta_manager.table_exists(bronze_whales_path):
            logger.warning("Bronze whale holders table not found")
            return {"status": "no_source", "records": 0}
        
        bronze_df = delta_manager.spark.read.format("delta").load(bronze_whales_path)
        
        # Apply silver layer transformations (similar to dbt model)
        filtered_df = bronze_df.filter(
            col("wallet_address").isNotNull() &
            col("token_address").isNotNull() &
            (col("holdings_amount") > 0)
        )
        
        # Add whale tiers and processing fields
        window_spec = Window.partitionBy("wallet_address", "token_address").orderBy(col("rank_date").desc(), col("_delta_timestamp").desc())
        
        silver_df = filtered_df.withColumn(
            "whale_id", concat(col("wallet_address"), lit("_"), col("token_address"))
        ).withColumn(
            "whale_tier",
            when(col("holdings_amount") >= 1000000, "MEGA")
            .when(col("holdings_amount") >= 100000, "LARGE")
            .when(col("holdings_amount") >= 10000, "MEDIUM")
            .when(col("holdings_amount") >= 1000, "SMALL")
            .otherwise("MINIMAL")
        ).withColumn(
            "rank_tier",
            when(col("rank") <= 3, "TOP_3")
            .when(col("rank") <= 10, "TOP_10")
            .when(col("rank") <= 50, "TOP_50")
            .otherwise("OTHER")
        ).withColumn(
            "processing_status",
            when(col("txns_fetched") == True, "completed")
            .when(col("txns_fetch_status") == "pending", "pending")
            .otherwise("ready")
        ).withColumn(
            "is_newly_tracked", lit(True)
        ).withColumn(
            "silver_created_at", current_timestamp()
        ).withColumn(
            "rn", row_number().over(window_spec)
        ).filter(col("rn") == 1).drop("rn")
        
        # Write to silver tracked whales Delta table
        silver_whales_path = "s3a://smart-trader/silver/tracked_whales_delta"
        version = delta_manager.create_table(
            silver_df,
            silver_whales_path,
            partition_cols=None,  # No partitioning for silver layer
            merge_schema=True
        )
        
        record_count = silver_df.count()
        logger.info(f"✅ Silver tracked whales: {record_count} records created in Delta v{version}")
        
        return {
            "status": "success",
            "records": record_count, 
            "delta_version": version,
            "table_path": silver_whales_path
        }
        
    except Exception as e:
        logger.error(f"❌ Silver tracked whales failed: {str(e)}")
        raise
    finally:
        if 'delta_manager' in locals():
            delta_manager.stop()

@task
def fetch_bronze_whales(**context):
    """Fetch whale data for tokens to smart-trader bucket with TRUE Delta Lake"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.optimized_delta_tasks import create_bronze_whales_delta
        result = create_bronze_whales_delta(**context)
        
        if result and isinstance(result, dict):
            whales_count = result.get('total_whales', 0)
            tokens_processed = result.get('tokens_processed', 0)
            logger.info(f"✅ Bronze whales: {tokens_processed} tokens, {whales_count} whales with Delta Lake")
            return {"status": "success", "whales_count": whales_count, "tokens_processed": tokens_processed}
        else:
            logger.warning("⚠️ No whale data returned")
            return {"status": "no_data", "whales_count": 0}
            
    except Exception as e:
        if any(keyword in str(e).lower() for keyword in API_RATE_LIMIT_KEYWORDS):
            logger.error("❌ BirdEye API rate limit exceeded")
        else:
            logger.error(f"❌ Bronze whales failed: {str(e)}")
        raise

@task
def fetch_bronze_transactions(**context):
    """Fetch transaction history for whale wallets to smart-trader bucket with TRUE Delta Lake"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.optimized_delta_tasks import create_bronze_transactions_delta
        result = create_bronze_transactions_delta(**context)
        
        if result and isinstance(result, dict):
            wallets_processed = result.get('wallets_processed', 0)
            transactions_saved = result.get('total_transactions', 0)
            logger.info(f"✅ Bronze transactions: {wallets_processed} wallets, {transactions_saved} transactions with Delta Lake")
            return {"status": "success", "wallets_processed": wallets_processed, "transactions_saved": transactions_saved}
        else:
            logger.warning("⚠️ No transaction data returned")
            return {"status": "no_data", "transactions_saved": 0}
            
    except Exception as e:
        if any(keyword in str(e).lower() for keyword in API_TIMEOUT_KEYWORDS):
            logger.error("❌ BirdEye API timeout")
        else:
            logger.error(f"❌ Bronze transactions failed: {str(e)}")
        raise

@task
def calculate_silver_pnl(**context):
    """Calculate wallet PnL using TRUE Delta Lake with PySpark FIFO methodology"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.optimized_delta_tasks import create_silver_wallet_pnl_delta
        result = create_silver_wallet_pnl_delta(**context)
        
        if result and isinstance(result, dict):
            wallets_processed = result.get('wallets_processed', 0)
            pnl_records = result.get('total_records', 0)
            logger.info(f"✅ Silver PnL: {wallets_processed} wallets, {pnl_records} records with Delta Lake FIFO")
            return {
                "status": "success",
                "wallets_processed": wallets_processed,
                "pnl_records": pnl_records
            }
        else:
            logger.warning("⚠️ No PnL data generated")
            return {"status": "no_data", "wallets_processed": 0, "pnl_records": 0}
            
    except Exception as e:
        if any(keyword in str(e).lower() for keyword in DATA_EMPTY_KEYWORDS):
            logger.error("❌ No transaction data available")
        elif any(keyword in str(e).lower() for keyword in STORAGE_ERROR_KEYWORDS):
            logger.error("❌ Storage access error")
        else:
            logger.error(f"❌ Silver PnL failed: {str(e)}")
        raise

@task
def generate_gold_traders(**context):
    """Generate smart trader rankings using TRUE Delta Lake with performance tiers"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.optimized_delta_tasks import create_gold_smart_traders_delta
        result = create_gold_smart_traders_delta(**context)
        
        if result and isinstance(result, dict):
            trader_count = result.get('smart_traders_count', 0)
            tier_breakdown = result.get('performance_tiers', {})
            
            logger.info(f"✅ Gold traders: {trader_count} identified, tiers: {tier_breakdown} with Delta Lake")
            return {
                "status": "success",
                "smart_traders_count": trader_count,
                "performance_tiers": tier_breakdown
            }
        else:
            logger.warning("⚠️ No smart traders identified")
            return {"status": "no_results", "smart_traders_count": 0}
            
    except Exception as e:
        if any(keyword in str(e).lower() for keyword in DBT_ERROR_KEYWORDS):
            logger.error("❌ Transformation failed")
        else:
            logger.error(f"❌ Gold traders failed: {str(e)}")
        raise

@task(trigger_rule=TriggerRule.ALL_DONE)
def update_helius_webhooks(**context):
    """Update Helius webhooks with top traders from Delta Lake (optional)"""
    logger = logging.getLogger(__name__)
    
    try:
        # This remains optional - can be implemented later
        logger.info("✅ Helius update: Skipped (optional step)")
        return {"status": "skipped", "addresses_updated": 0}
            
    except Exception as e:
        logger.warning(f"⚠️ Helius update failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

# DAG Definition
default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': DAG_DEPENDS_ON_PAST,
    'start_date': days_ago(DAG_START_DAYS_AGO),
    'email_on_failure': DAG_EMAIL_ON_FAILURE,
    'email_on_retry': DAG_EMAIL_ON_RETRY,
    'retries': DAG_RETRIES,
    'retry_delay': timedelta(minutes=DAG_RETRY_DELAY_MINUTES),
}

dag = DAG(
    'optimized_delta_smart_trader_identification',
    default_args=default_args,
    description='TRUE Delta Lake Smart Trader Pipeline: Bronze → Silver → Gold with ACID transactions',
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=['delta-lake', 'true-implementation', 'acid'] + DAG_TAGS,
)

# Task Dependencies - Correct Medallion Architecture
with dag:
    # Bronze layer - initial token fetch
    bronze_tokens = fetch_bronze_tokens()
    
    # Silver layer - filter tokens by liquidity
    silver_tracked_tokens = create_silver_tracked_tokens()
    
    # Bronze layer - fetch whale data for filtered tokens
    bronze_whales = fetch_bronze_whales()
    
    # Silver layer - process whale data
    silver_tracked_whales = create_silver_tracked_whales()
    
    # Bronze layer - fetch transaction data for whales
    bronze_transactions = fetch_bronze_transactions()
    
    # Silver layer - PnL analytics
    silver_pnl = calculate_silver_pnl()
    
    # Gold layer - smart trader identification
    gold_traders = generate_gold_traders()
    helius_update = update_helius_webhooks()
    
    # Dependencies: Correct flow
    bronze_tokens >> silver_tracked_tokens >> bronze_whales >> silver_tracked_whales >> bronze_transactions >> silver_pnl >> gold_traders >> helius_update