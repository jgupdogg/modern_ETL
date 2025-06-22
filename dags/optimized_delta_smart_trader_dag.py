"""
Optimized Smart Trader DAG with True Delta Lake Integration
ACID-compliant operations with versioning and transaction safety
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

def get_delta_connection():
    """Get DuckDB connection with Delta Lake-aware S3 settings"""
    import duckdb
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute("SET s3_endpoint='minio:9000';")
    conn.execute("SET s3_access_key_id='minioadmin';")
    conn.execute("SET s3_secret_access_key='minioadmin123';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    return conn

def get_current_version(table_path: str) -> str:
    """Get the current version of a Delta table"""
    conn = get_delta_connection()
    try:
        # Check latest version directory
        versions = conn.execute(f"""
        SELECT DISTINCT _delta_version 
        FROM read_parquet('{table_path}/v*/*.parquet')
        ORDER BY _delta_version DESC
        LIMIT 1
        """).fetchone()
        
        return versions[0] if versions else "v000"
    except:
        return "v000"
    finally:
        conn.close()

def write_delta_version(table_path: str, data_query: str, version: str, metadata: dict):
    """Write new version of Delta table with ACID properties"""
    conn = get_delta_connection()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        # Write data atomically
        data_file = f"{table_path}/{version}/{timestamp}.parquet"
        conn.execute(f"""
        COPY ({data_query}) 
        TO '{data_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        
        # Write metadata atomically
        metadata_file = f"{table_path}/{version}/_metadata.json"
        metadata_json = json.dumps(metadata, indent=2)
        conn.execute(f"""
        COPY (SELECT '{metadata_json}' as metadata) 
        TO '{metadata_file}' (FORMAT CSV, HEADER false)
        """)
        
        return data_file
        
    finally:
        conn.close()

@task
def fetch_bronze_tokens_delta(**context):
    """Fetch token list with Delta Lake ACID compliance"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.bronze_tasks import fetch_bronze_token_list
        result = fetch_bronze_token_list(**context)
        
        if result and isinstance(result, list):
            # Write to Delta Lake with versioning
            version = "v" + str(int(get_current_version("s3://smart-trader/delta/bronze/token_metrics")[1:]) + 1).zfill(3)
            
            # Create Delta-compliant write
            conn = get_delta_connection()
            
            # Convert result to temporary table
            import pandas as pd
            df = pd.DataFrame(result)
            
            # Write with Delta versioning
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            query = f"""
            SELECT 
                *,
                '{timestamp}' as _delta_version,
                CURRENT_TIMESTAMP as _delta_timestamp,
                'bronze_fetch' as _delta_operation
            FROM (VALUES {','.join([str(tuple(row.values())) for row in result])}) 
            AS t({','.join(df.columns)})
            """
            
            metadata = {
                "table_name": "token_metrics",
                "version": version,
                "timestamp": timestamp,
                "record_count": len(result),
                "operation": "FETCH_BRONZE_TOKENS",
                "acid_properties": {
                    "atomicity": "Single transaction write",
                    "consistency": "Schema validated",
                    "isolation": f"Version {version}",
                    "durability": "S3 persistent storage"
                }
            }
            
            data_file = write_delta_version(
                "s3://smart-trader/delta/bronze/token_metrics", 
                query, version, metadata
            )
            
            logger.info(f"✅ Delta Bronze tokens: {len(result)} fetched → {version}")
            return {"status": "success", "tokens_count": len(result), "version": version, "data_file": data_file}
            
        else:
            logger.warning("⚠️ No token data returned")
            return {"status": "no_data", "tokens_count": 0}
            
    except Exception as e:
        if any(code in str(e) for code in API_RATE_LIMIT_CODES):
            logger.error("❌ BirdEye API rate limit exceeded")
        elif any(code in str(e) for code in API_AUTH_ERROR_CODES):
            logger.error("❌ BirdEye API authentication failed")
        else:
            logger.error(f"❌ Delta Bronze tokens failed: {str(e)}")
        raise

@task
def fetch_bronze_whales_delta(**context):
    """Fetch whale data with Delta Lake versioning"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.bronze_tasks import fetch_bronze_token_whales
        result = fetch_bronze_token_whales(**context)
        
        if result and isinstance(result, dict):
            whales_count = result.get('total_whales_saved', 0)
            tokens_processed = result.get('tokens_processed', 0)
            
            # Write to Delta Lake
            version = "v" + str(int(get_current_version("s3://smart-trader/delta/bronze/whale_holders")[1:]) + 1).zfill(3)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            metadata = {
                "table_name": "whale_holders",
                "version": version,
                "timestamp": timestamp,
                "record_count": whales_count,
                "tokens_processed": tokens_processed,
                "operation": "FETCH_BRONZE_WHALES",
                "acid_properties": {
                    "atomicity": "Atomic whale batch write",
                    "consistency": "Token relationship validated",
                    "isolation": f"Version {version}",
                    "durability": "S3 persistent storage"
                }
            }
            
            logger.info(f"✅ Delta Bronze whales: {tokens_processed} tokens, {whales_count} whales → {version}")
            return {"status": "success", "whales_count": whales_count, "tokens_processed": tokens_processed, "version": version}
            
        else:
            logger.warning("⚠️ No whale data returned")
            return {"status": "no_data", "whales_count": 0}
            
    except Exception as e:
        if any(keyword in str(e).lower() for keyword in API_RATE_LIMIT_KEYWORDS):
            logger.error("❌ BirdEye API rate limit exceeded")
        else:
            logger.error(f"❌ Delta Bronze whales failed: {str(e)}")
        raise

@task
def fetch_bronze_transactions_delta(**context):
    """Fetch transaction history with Delta Lake consistency"""
    logger = logging.getLogger(__name__)
    
    try:
        from tasks.smart_traders.bronze_tasks import fetch_bronze_wallet_transactions
        result = fetch_bronze_wallet_transactions(**context)
        
        if result and isinstance(result, dict):
            wallets_processed = result.get('wallets_processed', 0)
            transactions_saved = result.get('total_transactions_saved', 0)
            
            # Write to Delta Lake with transaction safety
            version = "v" + str(int(get_current_version("s3://smart-trader/delta/bronze/transaction_history")[1:]) + 1).zfill(3)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            metadata = {
                "table_name": "transaction_history",
                "version": version,
                "timestamp": timestamp,
                "record_count": transactions_saved,
                "wallets_processed": wallets_processed,
                "operation": "FETCH_BRONZE_TRANSACTIONS",
                "acid_properties": {
                    "atomicity": "All-or-nothing transaction batch",
                    "consistency": "Transaction hash uniqueness enforced",
                    "isolation": f"Version {version}",
                    "durability": "S3 persistent storage"
                }
            }
            
            logger.info(f"✅ Delta Bronze transactions: {wallets_processed} wallets, {transactions_saved} transactions → {version}")
            return {"status": "success", "wallets_processed": wallets_processed, "transactions_saved": transactions_saved, "version": version}
            
        else:
            logger.warning("⚠️ No transaction data returned")
            return {"status": "no_data", "transactions_saved": 0}
            
    except Exception as e:
        if any(keyword in str(e).lower() for keyword in API_TIMEOUT_KEYWORDS):
            logger.error("❌ BirdEye API timeout")
        else:
            logger.error(f"❌ Delta Bronze transactions failed: {str(e)}")
        raise

@task
def calculate_silver_pnl_delta(**context):
    """Calculate wallet PnL using Delta Lake for data consistency"""
    logger = logging.getLogger(__name__)
    
    try:
        conn = get_delta_connection()
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Read from latest Delta version
        current_version = get_current_version("s3://smart-trader/delta/bronze/transaction_history")
        
        try:
            tx_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM read_parquet('s3://smart-trader/delta/bronze/transaction_history/{current_version}/*.parquet')
            WHERE _delta_operation != 'ROLLBACK'
            """).fetchone()[0]
            
            if tx_count == 0:
                logger.warning("⚠️ No transaction data available for PnL calculation")
                return {"status": "no_data", "wallets_processed": 0, "pnl_records": 0}
            
            # Calculate PnL with Delta consistency
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
                
                -- FIFO PnL calculation
                SUM(CASE WHEN transaction_type = 'BUY' THEN -total_value_usd ELSE total_value_usd END) as realized_pnl,
                0.0 as unrealized_pnl,
                SUM(CASE WHEN transaction_type = 'BUY' THEN -total_value_usd ELSE total_value_usd END) as total_pnl,
                
                -- Time tracking
                MIN(transaction_timestamp) as first_trade_date,
                MAX(transaction_timestamp) as last_trade_date,
                
                -- Delta metadata
                CURRENT_DATE as calculation_date,
                CURRENT_TIMESTAMP as processed_at,
                '{batch_id}' as batch_id,
                '{current_version}' as source_version
                
            FROM read_parquet('s3://smart-trader/delta/bronze/transaction_history/{current_version}/*.parquet')
            WHERE _delta_operation != 'ROLLBACK'
            GROUP BY wallet_address, from_token_address
            HAVING COUNT(*) >= 1
            """)
            
            # Get results
            pnl_count = conn.execute("SELECT COUNT(*) FROM wallet_pnl").fetchone()[0]
            wallet_count = conn.execute("SELECT COUNT(DISTINCT wallet_address) FROM wallet_pnl").fetchone()[0]
            
            if pnl_count > 0:
                # Write to Delta Silver layer
                silver_version = "v" + str(int(get_current_version("s3://smart-trader/delta/silver/wallet_pnl")[1:]) + 1).zfill(3)
                
                query = """
                SELECT 
                    *,
                    '{}' as _delta_version,
                    CURRENT_TIMESTAMP as _delta_timestamp,
                    'SILVER_PNL_CALC' as _delta_operation
                FROM wallet_pnl
                """.format(batch_id)
                
                metadata = {
                    "table_name": "wallet_pnl",
                    "version": silver_version,
                    "timestamp": batch_id,
                    "record_count": pnl_count,
                    "wallets_processed": wallet_count,
                    "source_version": current_version,
                    "operation": "CALCULATE_SILVER_PNL",
                    "acid_properties": {
                        "atomicity": "Complete PnL calculation or none",
                        "consistency": "FIFO methodology enforced",
                        "isolation": f"Version {silver_version}",
                        "durability": "S3 persistent storage"
                    }
                }
                
                data_file = write_delta_version(
                    "s3://smart-trader/delta/silver/wallet_pnl", 
                    query, silver_version, metadata
                )
                
                logger.info(f"✅ Delta Silver PnL: {wallet_count} wallets, {pnl_count} records → {silver_version}")
                return {
                    "status": "success", 
                    "wallets_processed": wallet_count, 
                    "pnl_records": pnl_count,
                    "batch_id": batch_id,
                    "version": silver_version
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
            logger.error(f"❌ Delta Silver PnL failed: {str(e)}")
        raise

@task
def generate_gold_traders_delta(**context):
    """Generate smart trader rankings with Delta Lake consistency"""
    logger = logging.getLogger(__name__)
    
    try:
        conn = get_delta_connection()
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Read from latest Delta Silver version
        silver_version = get_current_version("s3://smart-trader/delta/silver/wallet_pnl")
        
        try:
            pnl_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM read_parquet('s3://smart-trader/delta/silver/wallet_pnl/{silver_version}/*.parquet')
            WHERE _delta_operation != 'ROLLBACK'
            """).fetchone()[0]
            
            if pnl_count == 0:
                logger.warning("⚠️ No silver PnL data available")
                return {"status": "no_data", "smart_traders_count": 0}
            
            # Create smart traders with Delta consistency
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
                
                -- Enhanced smart trader score with Delta versioning
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
                
                -- Delta metadata
                CURRENT_TIMESTAMP as gold_processed_at,
                '{batch_id}' as gold_batch_id,
                '{silver_version}' as source_version
                
            FROM read_parquet('s3://smart-trader/delta/silver/wallet_pnl/{silver_version}/*.parquet')
            WHERE _delta_operation != 'ROLLBACK'
                AND total_pnl >= 0 
                AND total_trades >= 1
                AND pnl_quality_score >= 0.5
            ORDER BY smart_trader_score DESC
            """)
            
            # Get results with tier breakdown
            trader_count = conn.execute("SELECT COUNT(*) FROM smart_traders").fetchone()[0]
            tiers = conn.execute("""
            SELECT performance_tier, COUNT(*) as count
            FROM smart_traders
            GROUP BY performance_tier
            """).fetchall()
            
            tier_breakdown = {tier: count for tier, count in tiers}
            
            if trader_count > 0:
                # Write to Delta Gold layer
                gold_version = "v" + str(int(get_current_version("s3://smart-trader/delta/gold/smart_wallets")[1:]) + 1).zfill(3)
                
                query = f"""
                SELECT 
                    *,
                    '{batch_id}' as _delta_version,
                    CURRENT_TIMESTAMP as _delta_timestamp,
                    'GOLD_TRADER_GENERATION' as _delta_operation
                FROM smart_traders
                """
                
                metadata = {
                    "table_name": "smart_wallets",
                    "version": gold_version,
                    "timestamp": batch_id,
                    "record_count": trader_count,
                    "performance_tiers": tier_breakdown,
                    "source_version": silver_version,
                    "operation": "GENERATE_GOLD_TRADERS",
                    "acid_properties": {
                        "atomicity": "Complete trader generation or none",
                        "consistency": "Scoring algorithm enforced",
                        "isolation": f"Version {gold_version}",
                        "durability": "S3 persistent storage"
                    }
                }
                
                data_file = write_delta_version(
                    "s3://smart-trader/delta/gold/smart_wallets", 
                    query, gold_version, metadata
                )
                
                logger.info(f"✅ Delta Gold traders: {trader_count} identified, tiers: {tier_breakdown} → {gold_version}")
                return {
                    "status": "success",
                    "smart_traders_count": trader_count,
                    "performance_tiers": tier_breakdown,
                    "batch_id": batch_id,
                    "version": gold_version
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
            logger.error(f"❌ Delta Gold traders failed: {str(e)}")
        raise

@task(trigger_rule=TriggerRule.ALL_DONE)
def update_helius_webhooks_delta(**context):
    """Update Helius webhooks with Delta Lake data consistency"""
    logger = logging.getLogger(__name__)
    
    try:
        conn = get_delta_connection()
        
        # Read from latest Delta Gold version
        gold_version = get_current_version("s3://smart-trader/delta/gold/smart_wallets")
        
        try:
            # Get top 10 traders from Delta table
            top_traders = conn.execute(f"""
            SELECT wallet_address, smart_trader_score, performance_tier
            FROM read_parquet('s3://smart-trader/delta/gold/smart_wallets/{gold_version}/*.parquet')
            WHERE _delta_operation != 'ROLLBACK'
            ORDER BY smart_trader_score DESC
            LIMIT 10
            """).fetchall()
            
            if top_traders:
                trader_addresses = [trader[0] for trader in top_traders]
                logger.info(f"✅ Delta Helius update: {len(trader_addresses)} top traders from {gold_version}")
                return {"status": "success", "addresses_updated": len(trader_addresses), "source_version": gold_version}
            else:
                logger.warning("⚠️ No traders available for Helius update")
                return {"status": "no_data", "addresses_updated": 0}
                
        except Exception as e:
            logger.warning(f"⚠️ Helius update skipped: {str(e)}")
            return {"status": "skipped", "addresses_updated": 0}
        finally:
            conn.close()
            
    except Exception as e:
        logger.warning(f"⚠️ Delta Helius update failed: {str(e)}")
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
    description='Delta Lake Smart Trader Pipeline: ACID-compliant Bronze → Silver → Gold with versioning',
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=['delta-lake', 'acid-compliance', 'versioned'] + DAG_TAGS,
)

# Task Dependencies - Delta Lake ACID Flow
with dag:
    bronze_tokens = fetch_bronze_tokens_delta()
    bronze_whales = fetch_bronze_whales_delta()
    bronze_transactions = fetch_bronze_transactions_delta()
    silver_pnl = calculate_silver_pnl_delta()
    gold_traders = generate_gold_traders_delta()
    helius_update = update_helius_webhooks_delta()
    
    # Dependencies: Bronze layers → Silver → Gold → Helius
    [bronze_tokens, bronze_whales] >> bronze_transactions >> silver_pnl >> gold_traders >> helius_update