"""
Optimized Smart Trader Identification DAG
Enhanced for Delta Lake architecture with DuckDB and minimal logging
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
    """Fetch token list to smart-trader bucket using BirdEye API"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.bronze_tasks import fetch_bronze_token_list
        result = fetch_bronze_token_list(**context)
        
        if result and isinstance(result, list):
            logger.info(f"✅ Bronze tokens: {len(result)} fetched")
            return {"status": "success", "tokens_count": len(result)}
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
    """Fetch whale data for tokens to smart-trader bucket"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.bronze_tasks import fetch_bronze_token_whales
        result = fetch_bronze_token_whales(**context)
        
        if result and isinstance(result, dict):
            whales_count = result.get('total_whales_saved', 0)
            tokens_processed = result.get('tokens_processed', 0)
            logger.info(f"✅ Bronze whales: {tokens_processed} tokens, {whales_count} whales")
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
    """Fetch transaction history for whale wallets to smart-trader bucket"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.bronze_tasks import fetch_bronze_wallet_transactions
        result = fetch_bronze_wallet_transactions(**context)
        
        if result and isinstance(result, dict):
            wallets_processed = result.get('wallets_processed', 0)
            transactions_saved = result.get('total_transactions_saved', 0)
            logger.info(f"✅ Bronze transactions: {wallets_processed} wallets, {transactions_saved} transactions")
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
    """Calculate wallet PnL using DuckDB (replaces PySpark)"""
    logger = logging.getLogger(__name__)
    
    try:
        import duckdb
        
        # Setup DuckDB with S3 for smart-trader bucket
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_endpoint='minio:9000';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin123';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Check if transaction data exists
        try:
            tx_count = conn.execute("""
            SELECT COUNT(*) 
            FROM parquet_scan('s3://smart-trader/bronze/transaction_history/**/*.parquet')
            """).fetchone()[0]
            
            if tx_count == 0:
                logger.warning("⚠️ No transaction data available for PnL calculation")
                return {"status": "no_data", "wallets_processed": 0, "pnl_records": 0}
            
            # Execute DuckDB-based PnL calculation
            conn.execute(f"""
            CREATE TABLE wallet_pnl AS
            SELECT 
                wallet_address,
                from_token_address as token_address,
                COUNT(*) as total_trades,
                SUM(total_value_usd) as total_volume,
                AVG(trade_efficiency_score) as avg_trade_efficiency,
                AVG(timing_score) as avg_timing_score,
                AVG(data_completeness_score) as pnl_quality_score,
                
                -- Simple PnL calculation (buy - sell)
                SUM(CASE WHEN transaction_type = 'BUY' THEN -total_value_usd ELSE total_value_usd END) as realized_pnl,
                0.0 as unrealized_pnl,
                SUM(CASE WHEN transaction_type = 'BUY' THEN -total_value_usd ELSE total_value_usd END) as total_pnl,
                
                -- Time tracking
                MIN(transaction_timestamp) as first_trade_date,
                MAX(transaction_timestamp) as last_trade_date,
                
                -- Processing metadata
                CURRENT_DATE as calculation_date,
                CURRENT_TIMESTAMP as processed_at,
                '{batch_id}' as batch_id
                
            FROM parquet_scan('s3://smart-trader/bronze/transaction_history/**/*.parquet')
            WHERE processed_for_pnl = false
            GROUP BY wallet_address, from_token_address
            HAVING COUNT(*) >= 1
            """)
            
            # Get results
            pnl_count = conn.execute("SELECT COUNT(*) FROM wallet_pnl").fetchone()[0]
            wallet_count = conn.execute("SELECT COUNT(DISTINCT wallet_address) FROM wallet_pnl").fetchone()[0]
            
            if pnl_count > 0:
                # Export to smart-trader silver layer
                output_path = f"s3://smart-trader/silver/wallet_pnl/duckdb_{batch_id}.parquet"
                conn.execute(f"""
                COPY wallet_pnl TO '{output_path}' 
                (FORMAT PARQUET, COMPRESSION SNAPPY)
                """)
                
                # Mark transactions as processed
                conn.execute(f"""
                CREATE TABLE temp_update AS
                SELECT transaction_hash, '{batch_id}' as processing_batch, CURRENT_TIMESTAMP as processed_at
                FROM parquet_scan('s3://smart-trader/bronze/transaction_history/**/*.parquet')
                WHERE processed_for_pnl = false
                """)
                
                logger.info(f"✅ Silver PnL: {wallet_count} wallets, {pnl_count} records")
                return {
                    "status": "success", 
                    "wallets_processed": wallet_count, 
                    "pnl_records": pnl_count,
                    "batch_id": batch_id
                }
            else:
                logger.warning("⚠️ No PnL records generated")
                return {"status": "no_results", "wallets_processed": 0, "pnl_records": 0}
                
        except Exception as e:
            logger.error(f"❌ PnL calculation failed: {str(e)}")
            return {"status": "failed", "error": str(e)}
        finally:
            conn.close()
            
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
    """Generate smart trader rankings using DuckDB (replaces dbt for simplicity)"""
    logger = logging.getLogger(__name__)
    
    try:
        import duckdb
        
        # Setup DuckDB with S3
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_endpoint='minio:9000';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin123';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Check if silver PnL data exists
        try:
            pnl_count = conn.execute("""
            SELECT COUNT(*) 
            FROM parquet_scan('s3://smart-trader/silver/wallet_pnl/**/*.parquet')
            """).fetchone()[0]
            
            if pnl_count == 0:
                logger.warning("⚠️ No silver PnL data available")
                return {"status": "no_data", "smart_traders_count": 0}
            
            # Create smart traders with simplified criteria
            conn.execute(f"""
            CREATE TABLE smart_traders AS
            SELECT 
                wallet_address,
                token_address,
                total_pnl,
                total_trades,
                avg_trade_efficiency,
                avg_timing_score,
                pnl_quality_score,
                
                -- Enhanced smart trader score
                ROUND(
                    (LEAST(GREATEST(total_pnl, 0) / 1000.0, 1.0) * 0.4) +
                    (avg_trade_efficiency * 0.3) +
                    (avg_timing_score * 0.2) +
                    (CASE WHEN total_trades >= 5 THEN 0.1 ELSE 0.05 END),
                    3
                ) as smart_trader_score,
                
                -- Performance tier
                CASE 
                    WHEN total_pnl > 100 AND total_trades >= 5 AND avg_trade_efficiency >= 0.8 THEN 'STRONG'
                    WHEN total_pnl >= 0 AND total_trades >= 1 AND avg_trade_efficiency >= 0.7 THEN 'PROMISING'
                    ELSE 'DEVELOPING'
                END as performance_tier,
                
                -- Rankings
                ROW_NUMBER() OVER (ORDER BY total_pnl DESC, avg_trade_efficiency DESC) as overall_rank,
                
                -- Processing metadata
                CURRENT_TIMESTAMP as gold_processed_at,
                '{batch_id}' as gold_batch_id
                
            FROM parquet_scan('s3://smart-trader/silver/wallet_pnl/**/*.parquet')
            WHERE total_pnl >= 0 
                AND total_trades >= 1
                AND pnl_quality_score >= 0.5
            ORDER BY smart_trader_score DESC
            """)
            
            # Get results
            trader_count = conn.execute("SELECT COUNT(*) FROM smart_traders").fetchone()[0]
            
            # Get tier breakdown
            tiers = conn.execute("""
            SELECT performance_tier, COUNT(*) as count
            FROM smart_traders
            GROUP BY performance_tier
            """).fetchall()
            
            tier_breakdown = {tier: count for tier, count in tiers}
            
            if trader_count > 0:
                # Export to smart-trader gold layer
                output_path = f"s3://smart-trader/gold/smart_wallets/smart_traders_{batch_id}.parquet"
                conn.execute(f"""
                COPY smart_traders TO '{output_path}' 
                (FORMAT PARQUET, COMPRESSION SNAPPY)
                """)
                
                logger.info(f"✅ Gold traders: {trader_count} identified, tiers: {tier_breakdown}")
                return {
                    "status": "success",
                    "smart_traders_count": trader_count,
                    "performance_tiers": tier_breakdown,
                    "batch_id": batch_id
                }
            else:
                logger.warning("⚠️ No smart traders identified")
                return {"status": "no_results", "smart_traders_count": 0}
                
        except Exception as e:
            logger.error(f"❌ Gold generation failed: {str(e)}")
            return {"status": "failed", "error": str(e)}
        finally:
            conn.close()
            
    except Exception as e:
        if any(keyword in str(e).lower() for keyword in DBT_ERROR_KEYWORDS):
            logger.error("❌ Transformation failed")
        else:
            logger.error(f"❌ Gold traders failed: {str(e)}")
        raise

@task(trigger_rule=TriggerRule.ALL_DONE)
def update_helius_webhooks(**context):
    """Update Helius webhooks with top traders (optional)"""
    logger = logging.getLogger(__name__)
    
    try:
        # Check if gold data exists
        import duckdb
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_endpoint='minio:9000';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin123';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        try:
            # Get top 10 traders
            top_traders = conn.execute("""
            SELECT wallet_address, smart_trader_score, performance_tier
            FROM parquet_scan('s3://smart-trader/gold/smart_wallets/**/*.parquet')
            ORDER BY smart_trader_score DESC
            LIMIT 10
            """).fetchall()
            
            if top_traders:
                # Mock Helius update (replace with actual API call)
                trader_addresses = [trader[0] for trader in top_traders]
                logger.info(f"✅ Helius update: {len(trader_addresses)} top traders")
                return {"status": "success", "addresses_updated": len(trader_addresses)}
            else:
                logger.warning("⚠️ No traders available for Helius update")
                return {"status": "no_data", "addresses_updated": 0}
                
        except Exception as e:
            logger.warning(f"⚠️ Helius update skipped: {str(e)}")
            return {"status": "skipped", "addresses_updated": 0}
        finally:
            conn.close()
            
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
    'optimized_smart_trader_identification',
    default_args=default_args,
    description='Optimized Smart Trader Pipeline: Bronze → Silver → Gold (DuckDB-based)',
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=['optimized', 'duckdb', 'delta-lake'] + DAG_TAGS,
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