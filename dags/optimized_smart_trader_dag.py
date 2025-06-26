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

# Import PySpark functions needed for state tracking
from pyspark.sql.functions import col, lit, when, current_timestamp

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
            tokens_count = result.get('records', 0)
            new_records = result.get('new_records', 0)
            updated_records = result.get('updated_records', 0)
            logger.info(f"✅ Bronze tokens: {tokens_count} processed with Delta Lake ({new_records} new, {updated_records} updated)")
            return {"status": "success", "tokens_count": tokens_count, "new_records": new_records, "updated_records": updated_records}
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
    """Create silver tracked tokens using dbt SQL logic with PySpark execution"""
    logger = logging.getLogger(__name__)
    
    try:
        from utils.true_delta_manager import TrueDeltaLakeManager, get_table_path
        from config.true_delta_config import get_table_config
        
        delta_manager = TrueDeltaLakeManager()
        
        # Read bronze tokens from Delta Lake
        bronze_tokens_path = get_table_path("bronze_tokens")
        if not delta_manager.table_exists(bronze_tokens_path):
            logger.warning("Bronze tokens table not found")
            return {"status": "no_source", "records": 0}
        
        # Create temp view for SQL processing
        bronze_df = delta_manager.spark.read.format("delta").load(bronze_tokens_path)
        bronze_df.createOrReplaceTempView("bronze_tokens")
        
        # Execute dbt SQL logic via Spark SQL
        silver_tokens_sql = """
        WITH basic_filtered_tokens AS (
            SELECT 
                token_address,
                symbol,
                name,
                decimals,
                liquidity,
                price,
                fdv,
                holder,
                _delta_timestamp,
                _delta_operation,
                processing_date,
                batch_id,
                _delta_created_at
            FROM bronze_tokens
            WHERE 
                -- Only process unprocessed tokens
                processed = false
                
                -- Basic liquidity filter - only tokens with meaningful liquidity
                AND liquidity >= 100000  -- Min $100K liquidity for silver layer
                
                -- Ensure we have core required fields
                AND token_address IS NOT NULL
                AND symbol IS NOT NULL
                AND liquidity IS NOT NULL
                AND price IS NOT NULL
                
                -- Only process the latest Delta Lake operations
                AND _delta_operation IN ('BRONZE_TOKEN_CREATE', 'BRONZE_TOKEN_APPEND')
        ),
        
        enhanced_silver_tokens AS (
            SELECT 
                token_address,
                symbol,
                name,
                decimals,
                liquidity,
                price,
                fdv,
                holder,
                -- Calculate liquidity tier for categorization
                CASE 
                    WHEN liquidity >= 1000000 THEN 'HIGH'
                    WHEN liquidity >= 500000 THEN 'MEDIUM' 
                    WHEN liquidity >= 100000 THEN 'LOW'
                    ELSE 'MINIMAL'
                END as liquidity_tier,
                
                -- Calculate holder density (FDV per holder)
                CASE 
                    WHEN holder > 0 THEN ROUND(fdv / holder, 2)
                    ELSE NULL 
                END as fdv_per_holder,
                
                -- Silver layer metadata
                processing_date,
                batch_id as bronze_batch_id,
                _delta_timestamp as bronze_delta_timestamp,
                _delta_operation as source_operation,
                _delta_created_at as bronze_created_at,
                CURRENT_TIMESTAMP as silver_created_at,
                
                -- Tracking fields for downstream processing
                'pending' as whale_fetch_status,
                CAST(NULL as TIMESTAMP) as whale_fetched_at,
                true as is_newly_tracked,
                
                -- Row number for deduplication (keep latest by processing_date)
                ROW_NUMBER() OVER (
                    PARTITION BY token_address 
                    ORDER BY processing_date DESC, _delta_timestamp DESC
                ) as rn
            FROM basic_filtered_tokens
        )
        
        SELECT 
            token_address,
            symbol,
            name,
            decimals,
            liquidity,
            price,
            fdv,
            holder,
            liquidity_tier,
            fdv_per_holder,
            processing_date,
            bronze_batch_id,
            bronze_delta_timestamp,
            source_operation,
            bronze_created_at,
            silver_created_at,
            whale_fetch_status,
            whale_fetched_at,
            is_newly_tracked
        FROM enhanced_silver_tokens
        WHERE rn = 1  -- Keep only latest version of each token
        """
        
        # Execute SQL transformation
        silver_df = delta_manager.spark.sql(silver_tokens_sql)
        
        # Write to silver tracked tokens Delta table using MERGE
        silver_tokens_path = get_table_path("silver_tokens")
        table_config = get_table_config("silver_tokens")
        
        # Count records and track new vs updated
        record_count = silver_df.count()
        new_records = 0
        updated_records = 0
        
        if delta_manager.table_exists(silver_tokens_path):
            # Count existing tokens before merge to track new vs updated
            existing_tokens_df = delta_manager.spark.read.format("delta").load(silver_tokens_path)
            existing_token_addresses = [row.token_address for row in existing_tokens_df.select("token_address").collect()]
            silver_token_addresses = [row.token_address for row in silver_df.select("token_address").collect()]
            
            updated_records = len([addr for addr in silver_token_addresses if addr in existing_token_addresses])
            new_records = record_count - updated_records
            
            # Use MERGE operation (UPSERT) based on token_address
            # CRITICAL: Reset whale_fetch_status for both new and updated tokens
            merge_condition = "target.token_address = source.token_address"
            source_columns = [c for c in silver_df.columns if not c.startswith("_delta")]
            update_set = {c: f"source.{c}" for c in source_columns if c != "token_address"}
            # Force whale_fetch_status = pending on updates to ensure whales are refetched
            update_set["whale_fetch_status"] = "'pending'"
            update_set["whale_fetched_at"] = "null"
            update_set["is_newly_tracked"] = "true"
            
            insert_values = {c: f"source.{c}" for c in silver_df.columns}
            version = delta_manager.merge_data(
                silver_df, silver_tokens_path, 
                merge_condition, update_set, insert_values
            )
            operation = f"MERGE_UPSERT ({new_records} new, {updated_records} updated)"
        else:
            # Create table for first time
            new_records = record_count
            updated_records = 0
            version = delta_manager.create_table(
                silver_df, 
                silver_tokens_path,
                partition_cols=table_config["partition_cols"]
            )
            operation = "CREATE"
        
        logger.info(f"✅ Silver tracked tokens: {record_count} records → Delta v{version} ({operation})")
        logger.info(f"State management: All {record_count} tokens ready for whale fetching (whale_fetch_status=pending)")
        
        # Mark processed bronze tokens as completed
        if record_count > 0:
            processed_token_addresses = [row.token_address for row in silver_df.select("token_address").collect()]
            
            # Update bronze tokens to mark as processed
            bronze_tokens_path = get_table_path("bronze_tokens")
            if delta_manager.table_exists(bronze_tokens_path):
                bronze_df = delta_manager.spark.read.format("delta").load(bronze_tokens_path)
                
                # Create update DataFrame for MERGE operation
                from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
                
                # Create minimal update DataFrame with just the fields we need to update
                update_data = [
                    (token_address, True, datetime.now(), "BRONZE_TOKEN_UPDATE") 
                    for token_address in processed_token_addresses
                ]
                
                update_schema = StructType([
                    StructField("token_address", StringType(), False),
                    StructField("processed", BooleanType(), False),
                    StructField("_delta_timestamp", TimestampType(), False),
                    StructField("_delta_operation", StringType(), False)
                ])
                
                update_df = delta_manager.spark.createDataFrame(update_data, update_schema)
                
                # Use MERGE to update bronze tokens
                merge_condition = "target.token_address = source.token_address"
                update_set = {"processed": "source.processed", "_delta_timestamp": "source._delta_timestamp"}
                delta_manager.merge_data(
                    update_df, bronze_tokens_path,
                    merge_condition, update_set
                )
                logger.info(f"✅ Marked {len(processed_token_addresses)} bronze tokens as processed via MERGE")
        
        return {
            "status": "success", 
            "records": record_count,
            "new_records": new_records,
            "updated_records": updated_records,
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
    """Create silver tracked whales using dbt SQL logic with PySpark execution"""
    logger = logging.getLogger(__name__)
    
    try:
        from utils.true_delta_manager import TrueDeltaLakeManager, get_table_path
        from config.true_delta_config import get_table_config
        
        delta_manager = TrueDeltaLakeManager()
        
        # Read bronze whale holders from Delta Lake
        bronze_whales_path = get_table_path("bronze_whales")
        if not delta_manager.table_exists(bronze_whales_path):
            logger.warning("Bronze whale holders table not found")
            return {"status": "no_source", "records": 0}
        
        # Create temp view for SQL processing
        bronze_df = delta_manager.spark.read.format("delta").load(bronze_whales_path)
        bronze_df.createOrReplaceTempView("bronze_whales")
        
        # Execute dbt SQL logic via Spark SQL
        silver_whales_sql = """
        WITH basic_filtered_whales AS (
            SELECT 
                token_address,
                token_symbol,
                token_name,
                wallet_address,
                rank,
                amount,
                ui_amount,
                decimals,
                mint,
                token_account,
                fetched_at,
                batch_id,
                data_source,
                _delta_timestamp,
                _delta_operation,
                rank_date,
                _delta_created_at
            FROM bronze_whales
            WHERE 
                -- Basic validation filters
                wallet_address IS NOT NULL
                AND token_address IS NOT NULL
                AND ui_amount > 0  -- Only whales with actual token holdings
                
                -- Only process the latest Delta Lake operations
                AND _delta_operation IN ('BRONZE_WHALE_CREATE', 'BRONZE_WHALE_APPEND')
        ),
        
        enhanced_silver_whales AS (
            SELECT 
                -- Create composite whale ID for unique tracking
                CONCAT(wallet_address, '_', token_address) as whale_id,
                
                -- Core whale information
                wallet_address,
                token_address,
                token_symbol,
                token_name,
                rank,
                amount,
                ui_amount,
                decimals,
                mint,
                token_account,
                
                -- Calculate whale tier based on ui_amount (human readable amount)
                CASE 
                    WHEN ui_amount >= 1000000 THEN 'MEGA'
                    WHEN ui_amount >= 100000 THEN 'LARGE' 
                    WHEN ui_amount >= 10000 THEN 'MEDIUM'
                    WHEN ui_amount >= 1000 THEN 'SMALL'
                    ELSE 'MINIMAL'
                END as whale_tier,
                
                -- Calculate rank tier for easier filtering
                CASE 
                    WHEN rank <= 3 THEN 'TOP_3'
                    WHEN rank <= 10 THEN 'TOP_10'
                    WHEN rank <= 50 THEN 'TOP_50'
                    ELSE 'OTHER'
                END as rank_tier,
                
                -- Transaction tracking status (new for silver layer)
                false as txns_fetched,
                CAST(NULL as STRING) as txns_last_fetched_at,
                'pending' as txns_fetch_status,
                
                -- PnL processing tracking (new for silver layer)
                false as pnl_processed,
                CAST(NULL as STRING) as pnl_last_processed_at,
                'pending' as pnl_processing_status,
                
                -- Silver layer metadata
                rank_date,
                batch_id as bronze_batch_id,
                _delta_timestamp as bronze_delta_timestamp,
                _delta_operation as source_operation,
                _delta_created_at as bronze_created_at,
                CURRENT_TIMESTAMP as silver_created_at,
                
                -- Tracking fields for downstream processing
                'ready' as processing_status,
                
                true as is_newly_tracked,
                
                -- Row number for deduplication (keep latest by rank_date and bronze timestamp)
                ROW_NUMBER() OVER (
                    PARTITION BY wallet_address, token_address 
                    ORDER BY rank_date DESC, _delta_timestamp DESC
                ) as rn
            FROM basic_filtered_whales
        )
        
        SELECT 
            whale_id,
            wallet_address,
            token_address,
            token_symbol,
            token_name,
            rank,
            amount,
            ui_amount,
            decimals,
            mint,
            token_account,
            whale_tier,
            rank_tier,
            txns_fetched,
            txns_last_fetched_at,
            txns_fetch_status,
            pnl_processed,
            pnl_last_processed_at,
            pnl_processing_status,
            processing_status,
            rank_date,
            bronze_batch_id,
            bronze_delta_timestamp,
            source_operation,
            bronze_created_at,
            silver_created_at,
            is_newly_tracked
        FROM enhanced_silver_whales
        WHERE rn = 1  -- Keep only latest version of each whale-token pair
        """
        
        # Execute SQL transformation
        silver_df = delta_manager.spark.sql(silver_whales_sql)
        
        # Write to silver tracked whales Delta table using MERGE
        silver_whales_path = get_table_path("silver_whales")
        table_config = get_table_config("silver_whales")
        
        # Count records and track new vs updated
        record_count = silver_df.count()
        new_records = 0
        updated_records = 0
        
        if delta_manager.table_exists(silver_whales_path):
            # Count existing whale_ids before merge to track new vs updated
            existing_whales_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
            existing_whale_ids = [row.whale_id for row in existing_whales_df.select("whale_id").collect()]
            current_whale_ids = [row.whale_id for row in silver_df.select("whale_id").collect()]
            
            updated_records = len([whale_id for whale_id in current_whale_ids if whale_id in existing_whale_ids])
            new_records = record_count - updated_records
            
            # Use MERGE operation (UPSERT) based on whale_id
            # CRITICAL: Reset transaction/PnL status for both new and updated whales
            merge_condition = "target.whale_id = source.whale_id"
            source_columns = [c for c in silver_df.columns if not c.startswith("_delta")]
            update_set = {c: f"source.{c}" for c in source_columns if c != "whale_id"}
            # Force reset processing status for updated whales to ensure transactions are refetched
            update_set["processing_status"] = "'ready'"
            update_set["is_newly_tracked"] = "true"
            
            insert_values = {c: f"source.{c}" for c in silver_df.columns}
            version = delta_manager.merge_data(
                silver_df, silver_whales_path, 
                merge_condition, update_set, insert_values
            )
            operation = f"MERGE_UPSERT ({new_records} new, {updated_records} updated)"
        else:
            # Create table for first time
            new_records = record_count
            updated_records = 0
            version = delta_manager.create_table(
                silver_df,
                silver_whales_path,
                partition_cols=table_config["partition_cols"],
                merge_schema=True
            )
            operation = "CREATE"
        
        logger.info(f"✅ Silver tracked whales: {record_count} records → Delta v{version} ({operation})")
        logger.info(f"State management: All {record_count} whales ready for transaction fetching (processing_status=ready)")
        
        return {
            "status": "success",
            "records": record_count,
            "new_records": new_records,
            "updated_records": updated_records,
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
            whales_count = result.get('records', 0)
            new_whales = result.get('new_records', 0) 
            updated_whales = result.get('updated_records', 0)
            tokens_processed = result.get('tokens_processed', 0)
            logger.info(f"✅ Bronze whales: {tokens_processed} tokens processed, {whales_count} whale records ({new_whales} new, {updated_whales} updated)")
            return {"status": "success", "whales_count": whales_count, "new_whales": new_whales, "updated_whales": updated_whales, "tokens_processed": tokens_processed}
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
            transactions_saved = result.get('records', 0)
            new_transactions = result.get('new_records', 0)
            updated_transactions = result.get('updated_records', 0)
            logger.info(f"✅ Bronze transactions: {wallets_processed} wallets processed, {transactions_saved} transaction records ({new_transactions} new, {updated_transactions} updated)")
            return {"status": "success", "wallets_processed": wallets_processed, "transactions_saved": transactions_saved, "new_transactions": new_transactions, "updated_transactions": updated_transactions}
        else:
            logger.warning("⚠️ No transaction data returned")
            return {"status": "no_data", "transactions_saved": 0}
            
    except Exception as e:
        if any(keyword in str(e).lower() for keyword in API_TIMEOUT_KEYWORDS):
            logger.error("❌ BirdEye API timeout")
        else:
            logger.error(f"❌ Bronze transactions failed: {str(e)}")
        raise

@task(
    execution_timeout=timedelta(minutes=45),  # 45 minute timeout for processing 400+ wallets
    retries=1,  # Allow 1 retry if timeout occurs
    retry_delay=timedelta(minutes=5)
)
def calculate_silver_pnl(**context):
    """Calculate wallet PnL using TRUE Delta Lake with PySpark FIFO methodology"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.optimized_delta_tasks import create_silver_wallet_pnl_delta
        result = create_silver_wallet_pnl_delta(**context)
        
        if result and isinstance(result, dict):
            total_wallets_found = result.get('total_wallets_found', 0)
            wallets_processed = result.get('wallets_processed', 0)
            wallets_failed = result.get('wallets_failed', 0)
            new_wallets = result.get('new_wallets_count', 0)
            updated_wallets = result.get('updated_wallets_count', 0)
            pnl_records = result.get('total_pnl_records', 0)
            timeout_reached = result.get('timeout_reached', False)
            
            if timeout_reached:
                logger.info(f"⏱️ Silver PnL (partial): {wallets_processed}/{total_wallets_found} wallets processed ({new_wallets} new, {updated_wallets} updated), {pnl_records} PnL records, {wallets_failed} failed")
            else:
                logger.info(f"✅ Silver PnL: {wallets_processed}/{total_wallets_found} wallets processed ({new_wallets} new, {updated_wallets} updated), {pnl_records} PnL records, {wallets_failed} failed")
            
            return {
                "status": "success",
                "total_wallets_found": total_wallets_found,
                "wallets_processed": wallets_processed,
                "wallets_failed": wallets_failed,
                "new_wallets": new_wallets,
                "updated_wallets": updated_wallets,
                "pnl_records": pnl_records,
                "timeout_reached": timeout_reached
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
def create_gold_smart_traders(**context):
    """Create gold smart traders using dbt SQL logic with PySpark execution"""
    logger = logging.getLogger(__name__)
    
    try:
        from utils.true_delta_manager import TrueDeltaLakeManager, get_table_path
        from config.true_delta_config import get_table_config
        from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
        
        delta_manager = TrueDeltaLakeManager()
        
        # Check if silver PnL data exists
        silver_pnl_path = get_table_path("silver_pnl")
        if not delta_manager.table_exists(silver_pnl_path):
            logger.warning("Silver PnL table not found")
            return {"status": "no_source", "records": 0}
        
        # Filter for wallets not yet processed for gold layer
        silver_df = delta_manager.spark.read.format("delta").load(silver_pnl_path)
        unprocessed_for_gold = silver_df.filter(
            (col("moved_to_gold") == False) |
            col("moved_to_gold").isNull()
        )
        
        # Check if there are any unprocessed wallets
        if not unprocessed_for_gold.take(1):
            logger.info("No new wallets to process for gold layer")
            return {"status": "no_new_data", "smart_traders_count": 0}
        
        # Create temp view for SQL processing with only unprocessed wallets
        unprocessed_for_gold.createOrReplaceTempView("silver_wallet_pnl")
        
        # Execute dbt SQL logic via Spark SQL (from smart_wallets.sql)
        gold_traders_sql = """
        WITH smart_wallets AS (
            SELECT 
                wallet_address,
                trade_count,
                trade_frequency_daily,
                avg_holding_time_hours,
                total_bought,
                total_sold,
                win_rate,
                first_transaction,
                last_transaction,
                current_position_tokens,
                realized_pnl,
                unrealized_pnl,
                total_pnl,
                portfolio_roi as roi,
                batch_id,
                processed_at,
                data_source,
                -- Simple performance classification
                CASE 
                    WHEN total_pnl > 1000 AND win_rate > 50 THEN 'ELITE'
                    WHEN total_pnl > 100 AND win_rate > 30 THEN 'STRONG'
                    WHEN total_pnl > 0 OR win_rate > 0 THEN 'QUALIFIED'
                    ELSE 'UNQUALIFIED'
                END as performance_tier,
                -- Processing metadata
                CAST(CURRENT_TIMESTAMP AS STRING) as gold_processed_at
            FROM silver_wallet_pnl
            WHERE 
                -- Focus on portfolio-level records only
                token_address = 'ALL_TOKENS' 
                
                -- Simple criteria: positive PnL OR positive win rate
                AND (total_pnl > 0 OR win_rate > 0)
        )
        
        SELECT *
        FROM smart_wallets
        ORDER BY total_pnl DESC, win_rate DESC
        """
        
        # Execute SQL transformation
        gold_df = delta_manager.spark.sql(gold_traders_sql)
        
        # Write to gold smart traders Delta table using MERGE
        gold_traders_path = get_table_path("gold_traders")
        table_config = get_table_config("gold_traders")
        
        # Count records and track new vs updated
        record_count = gold_df.count()
        new_records = 0
        updated_records = 0
        
        if delta_manager.table_exists(gold_traders_path):
            # Count existing traders before merge to track new vs updated
            existing_traders_df = delta_manager.spark.read.format("delta").load(gold_traders_path)
            existing_wallet_addresses = [row.wallet_address for row in existing_traders_df.select("wallet_address").collect()]
            current_wallet_addresses = [row.wallet_address for row in gold_df.select("wallet_address").collect()]
            
            updated_records = len([addr for addr in current_wallet_addresses if addr in existing_wallet_addresses])
            new_records = record_count - updated_records
            
            # Use MERGE operation (UPSERT) based on wallet_address
            merge_condition = "target.wallet_address = source.wallet_address"
            source_columns = [c for c in gold_df.columns if not c.startswith("_delta")]
            update_set = {c: f"source.{c}" for c in source_columns if c != "wallet_address"}
            insert_values = {c: f"source.{c}" for c in gold_df.columns}
            
            version = delta_manager.merge_data(
                gold_df, gold_traders_path, 
                merge_condition, update_set, insert_values
            )
            operation = f"MERGE_UPSERT ({new_records} new, {updated_records} updated)"
        else:
            # Create table for first time
            new_records = record_count
            updated_records = 0
            version = delta_manager.create_table(
                gold_df,
                gold_traders_path,
                partition_cols=table_config["partition_cols"]
            )
            operation = "CREATE"
        
        # Get stats
        tier_counts = gold_df.groupBy("performance_tier").count().collect()
        tier_stats = {row['performance_tier']: row['count'] for row in tier_counts}
        
        # Mark processed wallets in silver_pnl as moved_to_gold using MERGE
        if record_count > 0:
            processed_wallet_addresses = [row.wallet_address for row in gold_df.select("wallet_address").collect()]
            
            # Create update DataFrame for MERGE operation
            update_data = [
                (wallet_address, True, datetime.now(), "completed", "SILVER_PNL_UPDATE") 
                for wallet_address in processed_wallet_addresses
            ]
            
            update_schema = StructType([
                StructField("wallet_address", StringType(), False),
                StructField("moved_to_gold", BooleanType(), False),
                StructField("gold_processed_at", TimestampType(), False),
                StructField("gold_processing_status", StringType(), False),
                StructField("_delta_operation", StringType(), False)
            ])
            
            update_df = delta_manager.spark.createDataFrame(update_data, update_schema)
            
            # Use MERGE to update silver PnL table
            merge_condition = "target.wallet_address = source.wallet_address AND target.token_address = 'ALL_TOKENS'"
            update_set = {
                "moved_to_gold": "source.moved_to_gold", 
                "gold_processed_at": "source.gold_processed_at",
                "gold_processing_status": "source.gold_processing_status",
                "_delta_timestamp": "current_timestamp()"
            }
            delta_manager.merge_data(
                update_df, silver_pnl_path,
                merge_condition, update_set
            )
            logger.info(f"✅ Marked {len(processed_wallet_addresses)} wallets as moved_to_gold via MERGE")
        
        logger.info(f"✅ Gold smart traders: {record_count} records → Delta v{version} ({operation})")
        logger.info(f"Performance tiers: {tier_stats}")
        
        return {
            "status": "success",
            "records": record_count,
            "new_records": new_records,
            "updated_records": updated_records,
            "performance_tiers": tier_stats,
            "delta_version": version,
            "table_path": gold_traders_path
        }
        
    except Exception as e:
        logger.error(f"❌ Gold smart traders failed: {str(e)}")
        return {"status": "failed", "error": str(e)}
    finally:
        if 'delta_manager' in locals():
            delta_manager.stop()

@task(trigger_rule=TriggerRule.ALL_DONE)
def update_helius_webhooks(**context):
    """Update Helius webhooks with top traders from Delta Lake"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.helius_tasks import update_helius_webhook
        result = update_helius_webhook(**context)
        
        if result and isinstance(result, dict):
            status = result.get('status', 'unknown')
            addresses_count = result.get('addresses_count', 0)
            
            if status == "success":
                logger.info(f"✅ Helius webhook updated with {addresses_count} smart trader addresses")
                return {"status": "success", "addresses_updated": addresses_count}
            elif status == "skipped":
                reason = result.get('reason', 'unknown')
                logger.info(f"⚠️ Helius update skipped: {reason}")
                return {"status": "skipped", "reason": reason, "addresses_updated": 0}
            else:
                error = result.get('error', 'Unknown error')
                logger.warning(f"⚠️ Helius update failed: {error}")
                return {"status": "failed", "error": error, "addresses_updated": 0}
        else:
            logger.warning("⚠️ Helius update returned unexpected result")
            return {"status": "failed", "error": "Unexpected result format", "addresses_updated": 0}
            
    except Exception as e:
        logger.warning(f"⚠️ Helius update failed: {str(e)}")
        return {"status": "failed", "error": str(e), "addresses_updated": 0}

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
    
    # Gold layer - dbt transformation
    gold_traders = create_gold_smart_traders()
    helius_update = update_helius_webhooks()
    
    # Dependencies: Correct flow
    bronze_tokens >> silver_tracked_tokens >> bronze_whales >> silver_tracked_whales >> bronze_transactions >> silver_pnl >> gold_traders >> helius_update