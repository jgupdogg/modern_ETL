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

# Task Dependencies - Simplified Linear Flow
with dag:
    bronze_tokens = fetch_bronze_tokens()
    bronze_whales = fetch_bronze_whales()
    bronze_transactions = fetch_bronze_transactions()
    silver_pnl = calculate_silver_pnl()
    gold_traders = generate_gold_traders()
    helius_update = update_helius_webhooks()
    
    # Dependencies: Bronze layers can run in parallel, then silver, then gold
    [bronze_tokens, bronze_whales] >> bronze_transactions >> silver_pnl >> gold_traders >> helius_update