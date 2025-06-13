"""
Bronze Layer Ingestion DAG
Fetches token data from BirdEye API and stores in MinIO bronze layer
"""
import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from io import BytesIO

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Configuration
class TokenListConfig:
    """Configuration for token list fetching"""
    limit: int = 100
    min_liquidity: int = 200000
    max_liquidity: int = 1000000
    min_volume_1h_usd: int = 200000
    min_price_change_2h_percent: int = 10
    min_price_change_24h_percent: int = 30


# BirdEye API Client
class BirdEyeAPIClient:
    """Simple client for BirdEye API v3"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://public-api.birdeye.so"
        self.headers = {
            "X-API-KEY": api_key,
            "Accept": "application/json"
        }
    
    def get_token_list(self, **params) -> Dict[str, Any]:
        """Get token list from v3 endpoint"""
        endpoint = f"{self.base_url}/defi/v3/token/list"
        response = requests.get(endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()


# PyArrow Schema Definition
def get_token_schema() -> pa.Schema:
    """Define PyArrow schema for token data"""
    return pa.schema([
        # Basic token info
        pa.field("token_address", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("decimals", pa.int32(), nullable=True),
        pa.field("logo_uri", pa.string(), nullable=True),
        
        # Market data
        pa.field("market_cap", pa.float64(), nullable=True),
        pa.field("fdv", pa.float64(), nullable=True),
        pa.field("liquidity", pa.float64(), nullable=True),
        pa.field("last_trade_unix_time", pa.int64(), nullable=True),
        pa.field("holder", pa.int64(), nullable=True),
        pa.field("recent_listing_time", pa.int64(), nullable=True),
        
        # Price data
        pa.field("price", pa.float64(), nullable=True),
        pa.field("price_change_1h_percent", pa.float64(), nullable=True),
        pa.field("price_change_2h_percent", pa.float64(), nullable=True),
        pa.field("price_change_4h_percent", pa.float64(), nullable=True),
        pa.field("price_change_8h_percent", pa.float64(), nullable=True),
        pa.field("price_change_24h_percent", pa.float64(), nullable=True),
        
        # Volume data
        pa.field("volume_1h_usd", pa.float64(), nullable=True),
        pa.field("volume_2h_usd", pa.float64(), nullable=True),
        pa.field("volume_4h_usd", pa.float64(), nullable=True),
        pa.field("volume_8h_usd", pa.float64(), nullable=True),
        pa.field("volume_24h_usd", pa.float64(), nullable=True),
        
        # Volume change percentages
        pa.field("volume_1h_change_percent", pa.float64(), nullable=True),
        pa.field("volume_2h_change_percent", pa.float64(), nullable=True),
        pa.field("volume_4h_change_percent", pa.float64(), nullable=True),
        pa.field("volume_8h_change_percent", pa.float64(), nullable=True),
        pa.field("volume_24h_change_percent", pa.float64(), nullable=True),
        
        # Trade counts
        pa.field("trade_1h_count", pa.int64(), nullable=True),
        pa.field("trade_2h_count", pa.int64(), nullable=True),
        pa.field("trade_4h_count", pa.int64(), nullable=True),
        pa.field("trade_8h_count", pa.int64(), nullable=True),
        pa.field("trade_24h_count", pa.int64(), nullable=True),
        
        # Extensions (stored as string/JSON)
        pa.field("extensions", pa.string(), nullable=True),
        
        # Ingestion metadata
        pa.field("ingested_at", pa.timestamp('us'), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
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


def fetch_token_list_v3(**context):
    """
    Task to fetch token list from BirdEye API and store in MinIO bronze layer
    """
    logger = logging.getLogger(__name__)
    
    # Get configuration
    config = TokenListConfig()
    
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
    birdeye_client = BirdEyeAPIClient(api_key)
    s3_client = get_minio_client()
    
    # Prepare parameters
    filter_params = {
        "min_liquidity": config.min_liquidity,
        "max_liquidity": config.max_liquidity,
        "min_volume_1h_usd": config.min_volume_1h_usd,
        "min_price_change_2h_percent": config.min_price_change_2h_percent,
        "min_price_change_24h_percent": config.min_price_change_24h_percent
    }
    
    logger.info(f"Fetching tokens with filters: {filter_params}")
    
    # Pagination variables
    offset = 0
    limit_per_call = 100
    all_tokens = []
    has_more = True
    sleep_between_calls = 0.5
    
    # Fetch tokens with pagination
    while has_more and len(all_tokens) < config.limit:
        remaining = config.limit - len(all_tokens)
        current_limit = min(limit_per_call, remaining)
        
        logger.info(f"API call: offset={offset}, limit={current_limit}")
        
        try:
            # Make API call
            params = {
                "sort_by": "liquidity",
                "sort_type": "desc",
                "offset": offset,
                "limit": current_limit,
                **filter_params
            }
            
            response = birdeye_client.get_token_list(**params)
            
            # Extract tokens
            if isinstance(response, dict) and "data" in response and "items" in response["data"]:
                tokens_data = response["data"]["items"]
            else:
                logger.warning(f"Unexpected response structure")
                tokens_data = []
            
            if not tokens_data:
                has_more = False
                break
            
            all_tokens.extend(tokens_data)
            offset += len(tokens_data)
            
            if len(tokens_data) < current_limit:
                has_more = False
            
            # Rate limiting
            if has_more and len(all_tokens) < config.limit:
                time.sleep(sleep_between_calls)
                
        except Exception as e:
            logger.error(f"Error fetching tokens: {e}")
            raise
    
    logger.info(f"Fetched {len(all_tokens)} tokens")
    
    # Ensure we don't exceed limit
    all_tokens = all_tokens[:config.limit]
    
    # Convert to DataFrame for easy manipulation
    df = pd.json_normalize(all_tokens)
    
    # Rename 'address' column to 'token_address' if it exists
    if 'address' in df.columns:
        df.rename(columns={'address': 'token_address'}, inplace=True)
    
    # Add ingestion metadata
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    df['ingested_at'] = pd.Timestamp.utcnow()
    df['batch_id'] = batch_id
    
    # Convert extensions to JSON string if present
    if 'extensions' in df.columns:
        df['extensions'] = df['extensions'].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else None
        )
    
    # Ensure all columns from schema exist (fill missing with None)
    schema = get_token_schema()
    for field in schema:
        if field.name not in df.columns:
            df[field.name] = None
    
    # Select only columns in schema and reorder
    df = df[[field.name for field in schema]]
    
    # Filter out rows without token_address
    df = df[df['token_address'].notna()]
    
    logger.info(f"Processed {len(df)} valid tokens")
    
    # Convert to PyArrow table with schema
    table = pa.Table.from_pandas(df, schema=schema)
    
    # Prepare MinIO path
    date_partition = datetime.utcnow().strftime("date=%Y-%m-%d")
    file_name = f"token_list_v3_{batch_id}.parquet"
    s3_key = f"bronze/token_list_v3/{date_partition}/{file_name}"
    
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
        logger.info(f"Successfully wrote {len(df)} tokens to s3://solana-data/{s3_key}")
        
        # Write success marker
        success_key = f"bronze/token_list_v3/{date_partition}/_SUCCESS"
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
        "tokens_fetched": len(all_tokens),
        "tokens_stored": len(df),
        "s3_path": f"s3://solana-data/{s3_key}",
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
    'bronze_layer_ingestion',
    default_args=default_args,
    description='Ingest data from external APIs to bronze layer in MinIO',
    schedule_interval='@hourly',
    catchup=False,
    tags=['bronze', 'ingestion', 'birdeye'],
)

# Define tasks
fetch_token_list_task = PythonOperator(
    task_id='fetch_token_list_v3',
    python_callable=fetch_token_list_v3,
    provide_context=True,
    dag=dag,
)

# Future tasks can be added here:
# fetch_token_metadata_task = PythonOperator(...)
# fetch_trending_tokens_task = PythonOperator(...)
# fetch_wallet_trade_history_task = PythonOperator(...)

# Task dependencies (when more tasks are added)
# fetch_token_list_task >> trigger_silver_layer_task