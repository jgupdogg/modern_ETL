"""
Smart Trader Identification DAG
Consolidated end-to-end pipeline: Bronze → Silver → Gold → Helius
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Import core functions from individual DAGs
import sys
sys.path.append('/opt/airflow/dags')

# Import centralized configuration
from config.smart_trader_config import (
    get_spark_config, get_duckdb_validation_command,
    SILVER_PNL_RECENT_DAYS, SILVER_PNL_HISTORICAL_LIMIT, 
    SILVER_PNL_MIN_TRANSACTIONS, PNL_AMOUNT_PRECISION_THRESHOLD,
    PNL_BATCH_PROGRESS_INTERVAL, BRONZE_WALLET_TRANSACTIONS_PATH,
    # DAG Configuration
    DAG_SCHEDULE_INTERVAL, DAG_MAX_ACTIVE_RUNS, DAG_CATCHUP, DAG_RETRIES,
    DAG_RETRY_DELAY_MINUTES, DAG_START_DAYS_AGO, DAG_OWNER, DAG_DEPENDS_ON_PAST,
    DAG_EMAIL_ON_FAILURE, DAG_EMAIL_ON_RETRY, DAG_TAGS,
    # dbt Configuration  
    DBT_PROFILES_DIR, DBT_PROJECT_DIR, DBT_MODEL_NAME,
    # Error Handling
    API_RATE_LIMIT_CODES, API_AUTH_ERROR_CODES, API_NOT_FOUND_CODES,
    API_TIMEOUT_KEYWORDS, API_RATE_LIMIT_KEYWORDS, API_AUTH_KEYWORDS,
    # Data Processing Error Keywords
    DATA_EMPTY_KEYWORDS, DATA_THRESHOLD_KEYWORDS, PYSPARK_ERROR_KEYWORDS,
    STORAGE_ERROR_KEYWORDS, MEMORY_ERROR_KEYWORDS, DBT_ERROR_KEYWORDS,
    SQL_ERROR_KEYWORDS, NO_RESULTS_KEYWORDS
)

@task
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
        if any(code in str(e) for code in API_RATE_LIMIT_CODES) or any(keyword in str(e).lower() for keyword in API_RATE_LIMIT_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API rate limit exceeded")
        elif any(code in str(e) for code in API_AUTH_ERROR_CODES) or any(keyword in str(e).lower() for keyword in API_AUTH_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API authentication failed")
        elif any(keyword in str(e).lower() for keyword in API_TIMEOUT_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API timeout")
        else:
            logger.error(f"❌ BRONZE TOKENS FAILED: {str(e)}")
        raise

@task
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
        if any(keyword in str(e).lower() for keyword in DATA_EMPTY_KEYWORDS):
            logger.error("❌ CRITICAL: No bronze token data available for filtering")
        elif any(keyword in str(e).lower() for keyword in DATA_THRESHOLD_KEYWORDS):
            logger.error("❌ SILVER TOKENS: Performance threshold filtering failed")
        else:
            logger.error(f"❌ SILVER TOKENS FAILED: {str(e)}")
        raise

@task
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
        if any(code in str(e) for code in API_RATE_LIMIT_CODES) or any(keyword in str(e).lower() for keyword in API_RATE_LIMIT_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API rate limit exceeded (whale data)")
        elif any(code in str(e) for code in API_AUTH_ERROR_CODES) or any(keyword in str(e).lower() for keyword in API_AUTH_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API authentication failed (whale data)")
        elif any(keyword in str(e).lower() for keyword in API_TIMEOUT_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API timeout (whale data)")
        elif any(keyword in str(e).lower() for keyword in NO_RESULTS_KEYWORDS):
            logger.error("❌ CRITICAL: No tracked tokens available for whale fetching")
        else:
            logger.error(f"❌ BRONZE WHALES FAILED: {str(e)}")
        raise

@task
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
        if any(code in str(e) for code in API_RATE_LIMIT_CODES) or any(keyword in str(e).lower() for keyword in API_RATE_LIMIT_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API rate limit exceeded (transactions)")
        elif any(code in str(e) for code in API_AUTH_ERROR_CODES) or any(keyword in str(e).lower() for keyword in API_AUTH_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API authentication failed (transactions)")
        elif any(keyword in str(e).lower() for keyword in API_TIMEOUT_KEYWORDS):
            logger.error("❌ CRITICAL: BirdEye API timeout (transactions)")
        elif any(code in str(e) for code in API_NOT_FOUND_CODES):
            logger.error("❌ CRITICAL: BirdEye API endpoint not found (check wallet transactions endpoint)")
        elif any(keyword in str(e).lower() for keyword in NO_RESULTS_KEYWORDS):
            logger.error("❌ CRITICAL: No whale wallets available for transaction fetching")
        else:
            logger.error(f"❌ BRONZE TRANSACTIONS FAILED: {str(e)}")
        raise

@task
def silver_wallet_pnl_task(**context):
    """Task 5: Calculate wallet PnL metrics using PySpark"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.silver_tasks import transform_silver_wallet_pnl
        result = transform_silver_wallet_pnl(**context)
        
        # Log aggregated success info
        if result and isinstance(result, dict):
            wallets_processed = result.get('wallets_processed', 0)
            total_pnl_records = result.get('total_pnl_records', 0)
            batch_id = result.get('batch_id', 'unknown')
            status = result.get('status', 'unknown')
            
            if status == 'mock_success':
                logger.warning(f"⚠️ SILVER PnL: Used mock data for {wallets_processed} wallets (PySpark unavailable)")
            else:
                logger.info(f"✅ SILVER PnL: Processed {wallets_processed} wallets, generated {total_pnl_records} PnL records")
        else:
            logger.warning("⚠️ SILVER PnL: No PnL calculation result returned")
            
        return result
        
    except Exception as e:
        # Log major errors only
        if any(keyword in str(e).lower() for keyword in DATA_EMPTY_KEYWORDS):
            logger.error("❌ CRITICAL: No bronze transaction data available for PnL calculation")
        elif any(keyword in str(e).lower() for keyword in PYSPARK_ERROR_KEYWORDS):
            logger.error("❌ CRITICAL: PySpark session or configuration error")
        elif any(keyword in str(e).lower() for keyword in STORAGE_ERROR_KEYWORDS):
            logger.error("❌ CRITICAL: S3/MinIO storage access error")
        elif any(keyword in str(e).lower() for keyword in MEMORY_ERROR_KEYWORDS):
            logger.error("❌ CRITICAL: Memory allocation error - consider reducing batch size")
        else:
            logger.error(f"❌ SILVER PnL FAILED: {str(e)}")
        raise

@task
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
        env['DBT_PROFILES_DIR'] = DBT_PROFILES_DIR
        
        # Run dbt model
        result = subprocess.run(
            ['dbt', 'run', '--models', DBT_MODEL_NAME],
            cwd=DBT_PROJECT_DIR,
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
        validation_cmd = get_duckdb_validation_command()
        
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
        if any(keyword in str(e).lower() for keyword in DBT_ERROR_KEYWORDS):
            logger.error("❌ CRITICAL: dbt transformation failed (gold layer)")
        elif any(keyword in str(e).lower() for keyword in NO_RESULTS_KEYWORDS):
            logger.error("❌ CRITICAL: No silver PnL data available for gold transformation")
        elif any(keyword in str(e).lower() for keyword in SQL_ERROR_KEYWORDS):
            logger.error("❌ CRITICAL: Database/SQL error in gold transformation")
        else:
            logger.error(f"❌ GOLD TRADERS FAILED: {str(e)}")
        raise

@task(trigger_rule=TriggerRule.ALL_DONE)
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
        if any(keyword in str(e).lower() for keyword in API_AUTH_KEYWORDS) or any(code in str(e) for code in API_AUTH_ERROR_CODES):
            logger.error("❌ HELIUS: Authentication failed (webhook update)")
        elif any(keyword in str(e).lower() for keyword in API_RATE_LIMIT_KEYWORDS) or any(code in str(e) for code in API_RATE_LIMIT_CODES):
            logger.error("❌ HELIUS: Rate limit exceeded (webhook update)")
        elif any(keyword in str(e).lower() for keyword in API_TIMEOUT_KEYWORDS):
            logger.error("❌ HELIUS: API timeout (webhook update)")
        elif any(keyword in str(e).lower() for keyword in NO_RESULTS_KEYWORDS):
            logger.warning("⚠️ HELIUS: No smart traders available for webhook update")
        else:
            logger.error(f"❌ HELIUS UPDATE FAILED: {str(e)}")
        
        # Don't re-raise since this is non-critical
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
    'smart_trader_identification',
    default_args=default_args,
    description='End-to-end smart trader identification pipeline: Bronze → Silver → Gold → Helius',
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=DAG_TAGS,
)

# Task Dependencies
with dag:
    # Call all @task decorated functions
    task_1_bronze_tokens = bronze_token_list_task()
    task_2_silver_tracked = silver_tracked_tokens_task() 
    task_3_bronze_whales = bronze_token_whales_task()
    task_4_bronze_transactions = bronze_wallet_transactions_task()
    pnl_task = silver_wallet_pnl_task()
    task_6_gold_traders = gold_top_traders_task()
    task_7_helius_update = helius_webhook_update_task()
    
    # Set up task dependencies
    task_1_bronze_tokens >> task_2_silver_tracked >> task_3_bronze_whales >> task_4_bronze_transactions >> pnl_task >> task_6_gold_traders >> task_7_helius_update

# Pipeline: Bronze Token List → Silver Tracked Tokens → Bronze Whales → Bronze Transactions → Silver PnL → Gold Traders → Helius Update