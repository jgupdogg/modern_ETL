"""
Helius Integration Tasks

Core business logic for updating Helius webhooks with top trader addresses.
Extracted from webhook DAGs for use in the smart trader identification pipeline.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from io import BytesIO

import pandas as pd
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
import requests
from airflow.models import Variable

# Import centralized configuration
from config.smart_trader_config import (
    HELIUS_API_BASE_URL, HELIUS_MAX_ADDRESSES, HELIUS_WEBHOOK_TYPE,
    HELIUS_TRANSACTION_TYPES, HELIUS_TIER_PRIORITY, HELIUS_REQUEST_TIMEOUT,
    GOLD_TOP_TRADERS_PATH
)
# Import Delta Lake MinIO configuration
from config.true_delta_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )


def read_latest_gold_traders() -> List[Dict[str, Any]]:
    """Read the latest gold smart traders using DuckDB"""
    logger = logging.getLogger(__name__)
    
    try:
        import duckdb
        
        # Create DuckDB connection
        conn = duckdb.connect(':memory:')
        
        # Install and load httpfs for S3 access
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Configure S3 settings for MinIO
        conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT.replace('http://', '')}';")
        conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
        conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        # Read the entire gold smart traders table
        query = f"""
        SELECT 
            wallet_address,
            performance_tier,
            total_pnl,
            win_rate,
            trade_count,
            roi
        FROM parquet_scan('s3://{MINIO_BUCKET}/gold/smart_traders_delta/**/*.parquet')
        ORDER BY 
            CASE 
                WHEN performance_tier = 'ELITE' THEN 1
                WHEN performance_tier = 'STRONG' THEN 2  
                WHEN performance_tier = 'QUALIFIED' THEN 3
                ELSE 4
            END,
            total_pnl DESC
        """
        
        # Execute query and fetch results
        result = conn.execute(query).fetchall()
        columns = [desc[0] for desc in conn.description]
        
        # Convert to list of dicts
        all_traders = [dict(zip(columns, row)) for row in result]
        
        conn.close()
        
        if not all_traders:
            logger.warning("No gold smart traders found in Delta Lake table")
            return []
        
        logger.info(f"Total gold traders read via DuckDB: {len(all_traders)}")
        
        # Log performance tier breakdown
        tier_counts = {}
        for trader in all_traders:
            tier = trader.get('performance_tier', 'UNKNOWN')
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
        logger.info(f"Performance tier breakdown: {tier_counts}")
        
        return all_traders
        
    except Exception as e:
        logger.error(f"Error reading gold traders via DuckDB: {e}")
        return []


def get_helius_api_key() -> str:
    """Get Helius API key from Airflow Variables"""
    try:
        return Variable.get('HELIUS_API_KEY')
    except:
        # Fallback to environment variable
        api_key = os.environ.get('HELIUS_API_KEY')
        if not api_key:
            raise ValueError("HELIUS_API_KEY not found in Airflow Variables or environment")
        return api_key


def get_current_webhook(api_key: str) -> Optional[Dict[str, Any]]:
    """Get current webhook configuration from Helius"""
    logger = logging.getLogger(__name__)
    
    
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.get(
            f"{HELIUS_API_BASE_URL}/webhooks?api-key={api_key}",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        webhooks = response.json()
        if not webhooks:
            logger.warning("No webhooks found in Helius account")
            return None
            
        # Use the first webhook
        webhook = webhooks[0]
        logger.info(f"Found webhook: {webhook.get('webhookID')}")
        return webhook
        
    except Exception as e:
        logger.error(f"Error fetching webhook from Helius: {e}")
        return None


def create_webhook(api_key: str, webhook_url: str, addresses: List[str]) -> Optional[str]:
    """Create a new webhook in Helius"""
    logger = logging.getLogger(__name__)
    
    
    try:
        headers = {"Content-Type": "application/json"}
        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": HELIUS_TRANSACTION_TYPES,
            "accountAddresses": addresses,
            "webhookType": HELIUS_WEBHOOK_TYPE
        }
        
        response = requests.post(
            f"{HELIUS_API_BASE_URL}/webhooks?api-key={api_key}",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        webhook_id = result.get('webhookID')
        logger.info(f"Created new webhook: {webhook_id}")
        return webhook_id
        
    except Exception as e:
        logger.error(f"Error creating webhook: {e}")
        return None


def update_webhook(api_key: str, webhook_id: str, webhook_url: str, addresses: List[str]) -> bool:
    """Update existing webhook with new addresses"""
    logger = logging.getLogger(__name__)
    
    
    try:
        headers = {"Content-Type": "application/json"}
        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": HELIUS_TRANSACTION_TYPES,
            "accountAddresses": addresses,
            "webhookType": HELIUS_WEBHOOK_TYPE
        }
        
        response = requests.put(
            f"{HELIUS_API_BASE_URL}/webhooks/{webhook_id}?api-key={api_key}",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        logger.info(f"Successfully updated webhook {webhook_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating webhook: {e}")
        return False


def update_helius_webhook(**context):
    """
    Update Helius webhook with addresses from gold top traders
    
    This is the main task function called by the DAG.
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Step 1: Read latest gold traders
        logger.info("Reading latest gold top traders...")
        gold_traders = read_latest_gold_traders()
        
        if not gold_traders:
            logger.warning("No gold traders found, skipping webhook update")
            return {
                "status": "skipped",
                "reason": "no_gold_traders",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        # Extract unique wallet addresses
        wallet_addresses = list(set(
            trader['wallet_address'] 
            for trader in gold_traders 
            if trader.get('wallet_address')
        ))
        
        # Limit to max addresses per webhook
        
        if len(wallet_addresses) > HELIUS_MAX_ADDRESSES:
            logger.info(f"Limiting addresses from {len(wallet_addresses)} to {HELIUS_MAX_ADDRESSES}")
            wallet_addresses = wallet_addresses[:HELIUS_MAX_ADDRESSES]
        
        logger.info(f"Found {len(wallet_addresses)} unique wallet addresses to monitor")
        
        # Step 2: Get Helius API key
        api_key = get_helius_api_key()
        
        # Step 3: Get or create webhook
        current_webhook = get_current_webhook(api_key)
        
        if current_webhook:
            webhook_id = current_webhook.get('webhookID')
            webhook_url = current_webhook.get('webhookURL')
            current_addresses = current_webhook.get('accountAddresses', [])
            
            logger.info(f"Current webhook has {len(current_addresses)} addresses")
            
            # Update existing webhook
            success = update_webhook(api_key, webhook_id, webhook_url, wallet_addresses)
            
            if not success:
                raise Exception("Failed to update webhook")
                
        else:
            # Create new webhook - need webhook URL from Variable
            try:
                webhook_url = Variable.get('HELIUS_WEBHOOK_URL')
            except:
                logger.error("No existing webhook and HELIUS_WEBHOOK_URL not set")
                return {
                    "status": "failed",
                    "reason": "no_webhook_url",
                    "timestamp": datetime.utcnow().isoformat()
                }
            
            webhook_id = create_webhook(api_key, webhook_url, wallet_addresses)
            if not webhook_id:
                raise Exception("Failed to create webhook")
        
        # Step 4: Update tracking variables
        Variable.set("helius_webhook_addresses_count", str(len(wallet_addresses)))
        Variable.set("helius_webhook_last_update", datetime.utcnow().isoformat())
        Variable.set("helius_webhook_id", webhook_id)
        
        # Get performance tier breakdown
        tier_breakdown = {}
        for trader in gold_traders[:len(wallet_addresses)]:
            tier = trader.get('performance_tier', 'unknown')
            tier_breakdown[tier] = tier_breakdown.get(tier, 0) + 1
        
        logger.info(f"✅ Successfully updated Helius webhook with {len(wallet_addresses)} addresses")
        logger.info(f"Performance tier breakdown: {tier_breakdown}")
        
        return {
            "status": "success",
            "webhook_id": webhook_id,
            "addresses_count": len(wallet_addresses),
            "performance_tiers": tier_breakdown,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in Helius webhook update: {e}")
        # Store error in Variable for monitoring
        Variable.set("helius_webhook_last_error", str(e))
        Variable.set("helius_webhook_last_error_time", datetime.utcnow().isoformat())
        
        # Return error info but don't raise - this is non-critical
        return {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }