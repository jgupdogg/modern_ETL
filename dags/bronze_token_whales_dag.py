"""
Bronze Token Whales DAG
Fetches top holder (whale) data for tracked tokens from BirdEye API
"""
import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import the birdeye client from local dags directory
from birdeye_client import BirdEyeAPIClient


# Configuration
class WhaleConfig:
    """Configuration for whale data fetching"""
    max_whales_per_token: int = 20  # Top N holders to fetch
    batch_limit: int = 50  # Tokens per DAG run
    refresh_days: int = 7  # Re-fetch if older than N days
    rate_limit_delay: float = 0.5  # Seconds between API calls


def get_whale_schema() -> pa.Schema:
    """Get PyArrow schema for whale data"""
    return pa.schema([
        # Token identification
        pa.field("token_address", pa.string(), nullable=False),
        pa.field("token_symbol", pa.string(), nullable=True),
        pa.field("token_name", pa.string(), nullable=True),
        
        # Whale/holder information
        pa.field("wallet_address", pa.string(), nullable=False),
        pa.field("rank", pa.int32(), nullable=True),  # 1st, 2nd, 3rd largest holder
        pa.field("holdings_amount", pa.float64(), nullable=True),
        pa.field("holdings_value_usd", pa.float64(), nullable=True),
        pa.field("holdings_percentage", pa.float64(), nullable=True),
        
        # Transaction tracking fields (for future use)
        pa.field("txns_fetched", pa.bool_(), nullable=False),  # Default: False
        pa.field("txns_last_fetched_at", pa.timestamp('us', tz='UTC'), nullable=True),
        pa.field("txns_fetch_status", pa.string(), nullable=True),  # pending/completed/failed
        
        # Metadata
        pa.field("fetched_at", pa.timestamp('us', tz='UTC'), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("data_source", pa.string(), nullable=False),  # 'birdeye_v3'
    ])


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4')
    )


def query_tracked_tokens(batch_limit: int, refresh_days: int) -> pd.DataFrame:
    """Query tracked tokens needing whale data - simplified test version"""
    # For testing, return hardcoded tokens from our silver table
    # In production, this would query DuckDB
    test_tokens = [
        {
            'token_address': 'So11111111111111111111111111111111111111112',
            'symbol': 'SOL',
            'name': 'Solana'
        },
        {
            'token_address': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 
            'symbol': 'USDC',
            'name': 'USD Coin'
        },
        {
            'token_address': 'TEST456',
            'symbol': 'TST2', 
            'name': 'Test Token 2'
        }
    ]
    
    return pd.DataFrame(test_tokens[:batch_limit])


def fetch_token_whales(**context):
    """
    Task to fetch whale data for tracked tokens and store in MinIO bronze layer
    """
    logger = logging.getLogger(__name__)
    
    # Get configuration
    config = WhaleConfig()
    
    # Try to get API key from Airflow Variable first, then environment
    from airflow.models import Variable
    try:
        api_key = Variable.get('BIRDSEYE_API_KEY')
        logger.info("Using BIRDSEYE_API_KEY from Airflow Variable")
    except:
        api_key = os.environ.get('BIRDSEYE_API_KEY')
        if api_key:
            logger.info("Using BIRDSEYE_API_KEY from environment")
        
    if not api_key:
        raise ValueError("BIRDSEYE_API_KEY not found in Airflow Variables or environment")
    
    # Initialize clients
    birdeye_client = BirdEyeAPIClient(api_key, rate_limit_delay=config.rate_limit_delay)
    s3_client = get_minio_client()
    
    # Get tokens needing whale data from DuckDB
    try:
        logger.info(f"Querying for tokens needing whale data...")
        tokens_df = query_tracked_tokens(config.batch_limit, config.refresh_days)
        
        logger.info(f"Found {len(tokens_df)} tokens needing whale data")
        
        if tokens_df.empty:
            logger.info("No tokens need whale data at this time")
            return {
                "tokens_processed": 0,
                "whales_fetched": 0,
                "errors": 0
            }
        
    except Exception as e:
        logger.error(f"Error querying DuckDB: {e}")
        raise
    
    # Process each token
    all_whale_data = []
    tokens_processed = 0
    errors = 0
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fetched_at = pd.Timestamp.utcnow()
    
    for _, token_row in tokens_df.iterrows():
        token_address = token_row['token_address']
        token_symbol = token_row.get('symbol', token_address[:8])
        token_name = token_row.get('name', '')
        
        logger.info(f"Fetching whale data for {token_symbol} ({token_address})")
        
        try:
            # Fetch top holders from BirdEye API
            response = birdeye_client.get_token_top_holders(
                token_address=token_address,
                offset=0,
                limit=config.max_whales_per_token
            )
            
            # Parse response
            if response.get('success') and 'data' in response:
                holders_data = response['data'].get('items', response['data'].get('holders', []))
                
                # Process each holder
                for idx, holder in enumerate(holders_data):
                    whale_record = {
                        # Token identification
                        "token_address": token_address,
                        "token_symbol": token_symbol,
                        "token_name": token_name,
                        
                        # Whale/holder information
                        "wallet_address": holder.get('owner', holder.get('wallet_address', '')),
                        "rank": idx + 1,
                        "holdings_amount": float(holder.get('uiAmount', holder.get('ui_amount', 0))),
                        "holdings_value_usd": float(holder.get('valueUsd', holder.get('value_usd', 0))) if holder.get('valueUsd', holder.get('value_usd')) else None,
                        "holdings_percentage": float(holder.get('percentage', 0)) if holder.get('percentage') else None,
                        
                        # Transaction tracking fields
                        "txns_fetched": False,
                        "txns_last_fetched_at": None,
                        "txns_fetch_status": "pending",
                        
                        # Metadata
                        "fetched_at": fetched_at,
                        "batch_id": batch_id,
                        "data_source": "birdeye_v3"
                    }
                    
                    all_whale_data.append(whale_record)
                
                logger.info(f"Fetched {len(holders_data)} whales for {token_symbol}")
                tokens_processed += 1
            else:
                logger.warning(f"No whale data returned for {token_symbol}")
                errors += 1
                
        except Exception as e:
            logger.error(f"Error fetching whales for {token_symbol}: {e}")
            errors += 1
            continue
        
        # Rate limiting
        if _ < len(tokens_df) - 1:  # Don't sleep after last token
            time.sleep(config.rate_limit_delay)
    
    # Convert to DataFrame and save to MinIO
    if all_whale_data:
        logger.info(f"Processing {len(all_whale_data)} total whale records")
        
        # Create DataFrame
        whales_df = pd.DataFrame(all_whale_data)
        
        # Ensure all columns from schema exist
        schema = get_whale_schema()
        for field in schema:
            if field.name not in whales_df.columns:
                whales_df[field.name] = None
        
        # Select only columns in schema and reorder
        whales_df = whales_df[[field.name for field in schema]]
        
        # Convert to PyArrow table with schema
        table = pa.Table.from_pandas(whales_df, schema=schema)
        
        # Prepare MinIO path
        date_partition = datetime.utcnow().strftime("date=%Y-%m-%d")
        file_name = f"token_whales_{batch_id}.parquet"
        s3_key = f"bronze/token_whales/{date_partition}/{file_name}"
        
        # Write to MinIO
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        try:
            s3_client.put_object(
                Bucket='solana-data',
                Key=s3_key,
                Body=buffer.getvalue()
            )
            logger.info(f"Successfully wrote {len(whales_df)} whale records to s3://solana-data/{s3_key}")
            
            # Write success marker
            success_key = f"bronze/token_whales/{date_partition}/_SUCCESS"
            s3_client.put_object(
                Bucket='solana-data',
                Key=success_key,
                Body=b''
            )
            
        except Exception as e:
            logger.error(f"Error writing to MinIO: {e}")
            raise
    
    # Return metadata
    return {
        "tokens_processed": tokens_processed,
        "whales_fetched": len(all_whale_data),
        "errors": errors,
        "s3_path": f"s3://solana-data/{s3_key}" if all_whale_data else None,
        "batch_id": batch_id
    }


# DAG Definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_token_whales',
    default_args=default_args,
    description='Fetch whale (top holder) data for tracked tokens from BirdEye API',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False,
    tags=['bronze', 'whales', 'birdeye'],
)

# Define tasks
fetch_whales_task = PythonOperator(
    task_id='fetch_token_whales',
    python_callable=fetch_token_whales,
    provide_context=True,
    dag=dag,
)

# This DAG fetches whale data for tokens marked as needing whale data in the silver layer