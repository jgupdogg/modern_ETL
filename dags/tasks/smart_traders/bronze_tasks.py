"""
Bronze Layer Tasks

Core business logic for bronze layer data ingestion tasks.
Extracted from individual bronze DAGs for use in the smart trader identification pipeline.
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

# Import the birdeye client from local dags directory
from birdeye_client import BirdEyeAPIClient, TokenListSchema

# Import centralized configuration
from config.smart_trader_config import (
    TOKEN_LIMIT, MIN_LIQUIDITY, MAX_LIQUIDITY, MIN_VOLUME_1H_USD,
    MIN_PRICE_CHANGE_2H_PERCENT, MIN_PRICE_CHANGE_24H_PERCENT,
    MAX_WHALES_PER_TOKEN, BRONZE_WHALE_BATCH_LIMIT, WHALE_REFRESH_DAYS,
    BRONZE_WALLET_BATCH_LIMIT, MAX_TRANSACTIONS_PER_WALLET,
    API_RATE_LIMIT_DELAY, WALLET_API_DELAY, API_PAGINATION_LIMIT,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    BRONZE_TOKEN_LIST_PATH, BRONZE_TOKEN_WHALES_PATH, BRONZE_WALLET_TRANSACTIONS_PATH
)


def get_token_schema() -> pa.Schema:
    """Get token schema from shared client"""
    return TokenListSchema.get_pyarrow_schema()


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )


def fetch_bronze_token_list(**context):
    """
    Task to fetch token list from BirdEye API and store in MinIO bronze layer
    
    Extracted from bronze_layer_ingestion_dag.py
    """
    logger = logging.getLogger(__name__)
    
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
    
    # Prepare parameters using centralized config
    filter_params = {
        "min_liquidity": MIN_LIQUIDITY,
        "max_liquidity": MAX_LIQUIDITY,
        "min_volume_1h_usd": MIN_VOLUME_1H_USD,
        "min_price_change_2h_percent": MIN_PRICE_CHANGE_2H_PERCENT,
        "min_price_change_24h_percent": MIN_PRICE_CHANGE_24H_PERCENT
    }
    
    logger.info(f"Fetching tokens with filters: {filter_params}")
    
    # Pagination variables using centralized config
    offset = 0
    limit_per_call = API_PAGINATION_LIMIT
    all_tokens = []
    has_more = True
    sleep_between_calls = API_RATE_LIMIT_DELAY
    
    # Fetch tokens with pagination
    while has_more and len(all_tokens) < TOKEN_LIMIT:
        remaining = TOKEN_LIMIT - len(all_tokens)
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
            
            # Use the client's normalization method
            tokens_data = birdeye_client.normalize_token_list_response(response)
            
            if not tokens_data:
                has_more = False
                break
            
            all_tokens.extend(tokens_data)
            offset += len(tokens_data)
            
            if len(tokens_data) < current_limit:
                has_more = False
            
            # Rate limiting
            if has_more and len(all_tokens) < TOKEN_LIMIT:
                time.sleep(sleep_between_calls)
                
        except Exception as e:
            logger.error(f"Error fetching tokens: {e}")
            raise
    
    logger.info(f"Fetched {len(all_tokens)} tokens")
    
    # Ensure we don't exceed limit
    all_tokens = all_tokens[:TOKEN_LIMIT]
    
    # Convert normalized tokens to DataFrame
    df = pd.DataFrame(all_tokens)
    
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


# Configuration classes replaced by centralized config
# (WhaleConfig now uses imported constants from config.smart_trader_config)


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


def query_tracked_tokens_for_whales(batch_limit: int, refresh_days: int) -> pd.DataFrame:
    """Query tracked tokens needing whale data"""
    # For now, use the latest silver tracked tokens from MinIO
    # In production, this would query DuckDB with more complex logic
    try:
        s3_client = get_minio_client()
        
        # List silver tracked token files
        response = s3_client.list_objects_v2(
            Bucket='solana-data',
            Prefix='silver/tracked_tokens/',
            MaxKeys=1000
        )
        
        if 'Contents' not in response:
            return pd.DataFrame()
        
        # Filter for parquet files and get the most recent
        parquet_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        if not parquet_files:
            return pd.DataFrame()
        
        latest_file = sorted(parquet_files, key=lambda x: x['LastModified'], reverse=True)[0]['Key']
        
        # Download and read the silver tokens
        obj_response = s3_client.get_object(Bucket='solana-data', Key=latest_file)
        parquet_data = obj_response['Body'].read()
        
        table = pq.read_table(BytesIO(parquet_data))
        df = table.to_pandas()
        
        # Select needed columns and limit
        if 'token_address' in df.columns:
            result = df[['token_address', 'symbol', 'name']].rename(columns={'symbol': 'symbol', 'name': 'name'})
            return result.head(batch_limit)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        # Fallback to test data if silver layer not available
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
            }
        ]
        return pd.DataFrame(test_tokens[:batch_limit])


def fetch_bronze_token_whales(**context):
    """
    Fetch whale data for tracked tokens from BirdEye API and store in MinIO bronze layer
    
    Extracted from bronze_token_whales_dag.py
    """
    logger = logging.getLogger(__name__)
    
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
    
    # Get tokens needing whale data using centralized config
    try:
        logger.info(f"Querying for tokens needing whale data...")
        tokens_df = query_tracked_tokens_for_whales(BRONZE_WHALE_BATCH_LIMIT, WHALE_REFRESH_DAYS)
        
        logger.info(f"Found {len(tokens_df)} tokens needing whale data")
        
        if tokens_df.empty:
            logger.info("No tokens need whale data at this time")
            return {
                "tokens_processed": 0,
                "whales_fetched": 0,
                "errors": 0
            }
        
    except Exception as e:
        logger.error(f"Error querying tokens: {e}")
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
                limit=MAX_WHALES_PER_TOKEN
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
            time.sleep(API_RATE_LIMIT_DELAY)
    
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


# Configuration classes replaced by centralized config
# (TransactionConfig now uses imported constants from config.smart_trader_config)




def get_raw_transaction_schema() -> pa.Schema:
    """Get PyArrow schema for raw wallet transaction data (new approach - stores raw swap data)"""
    return pa.schema([
        # Core identification
        pa.field("wallet_address", pa.string(), nullable=False),
        pa.field("transaction_hash", pa.string(), nullable=False), 
        pa.field("timestamp", pa.timestamp('us', tz='UTC'), nullable=False),
        
        # Raw API response - Base token
        pa.field("base_symbol", pa.string(), nullable=True),
        pa.field("base_address", pa.string(), nullable=True),
        pa.field("base_type_swap", pa.string(), nullable=True),  # "from" or "to"
        pa.field("base_ui_change_amount", pa.float64(), nullable=True),
        pa.field("base_nearest_price", pa.float64(), nullable=True),
        pa.field("base_decimals", pa.int32(), nullable=True),
        pa.field("base_ui_amount", pa.float64(), nullable=True),
        pa.field("base_change_amount", pa.string(), nullable=True),  # Raw amount as string
        
        # Raw API response - Quote token  
        pa.field("quote_symbol", pa.string(), nullable=True),
        pa.field("quote_address", pa.string(), nullable=True),
        pa.field("quote_type_swap", pa.string(), nullable=True),  # "from" or "to"
        pa.field("quote_ui_change_amount", pa.float64(), nullable=True),
        pa.field("quote_nearest_price", pa.float64(), nullable=True),
        pa.field("quote_decimals", pa.int32(), nullable=True),
        pa.field("quote_ui_amount", pa.float64(), nullable=True),
        pa.field("quote_change_amount", pa.string(), nullable=True),  # Raw amount as string
        
        # Transaction metadata
        pa.field("source", pa.string(), nullable=True),  # DEX source
        pa.field("tx_type", pa.string(), nullable=True),  # "swap", "transfer"
        pa.field("block_unix_time", pa.int64(), nullable=True),
        pa.field("owner", pa.string(), nullable=True),  # API owner field
        
        # Processing state tracking
        pa.field("processed_for_pnl", pa.bool_(), nullable=False),  # Default: False
        pa.field("pnl_processed_at", pa.timestamp('us', tz='UTC'), nullable=True),
        pa.field("pnl_processing_batch_id", pa.string(), nullable=True),
        
        # Metadata
        pa.field("fetched_at", pa.timestamp('us', tz='UTC'), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("data_source", pa.string(), nullable=False)  # "birdeye_v3", "migration"
    ])


def read_unfetched_whales(batch_limit: int) -> pd.DataFrame:
    """Read whale data from MinIO and filter for unfetched wallets"""
    logger = logging.getLogger(__name__)
    s3_client = get_minio_client()
    
    # List all whale parquet files
    whale_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket='solana-data', Prefix='bronze/token_whales/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                whale_files.append(obj['Key'])
    
    logger.info(f"Found {len(whale_files)} whale parquet files: {whale_files}")
    
    # Read and combine whale data
    all_whales = []
    for file_key in whale_files:
        try:
            logger.info(f"Reading whale file: {file_key}")
            obj = s3_client.get_object(Bucket='solana-data', Key=file_key)
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            logger.info(f"Successfully read {len(df)} rows from {file_key}, columns: {list(df.columns)}")
            
            # Check if required columns exist
            required_cols = ['wallet_address', 'token_address', 'txns_fetched', 'txns_fetch_status', 'fetched_at']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"File {file_key} missing required columns: {missing_cols}, skipping...")
                continue
            
            all_whales.append(df)
        except Exception as e:
            logger.warning(f"Error reading whale file {file_key}: {e}, skipping...")
            continue
    
    if not all_whales:
        logger.info("No valid whale files found, returning empty DataFrame")
        return pd.DataFrame()
    
    # Combine all whale data
    whales_df = pd.concat(all_whales, ignore_index=True)
    logger.info(f"Combined whale data: {len(whales_df)} total rows")
    
    # Filter for unfetched wallets
    unfetched = whales_df[
        (whales_df['txns_fetched'] == False) & 
        (whales_df['txns_fetch_status'] == 'pending')
    ]
    logger.info(f"Found {len(unfetched)} unfetched whale wallets")
    
    # Get unique wallet-token pairs
    unique_wallets = unfetched.drop_duplicates(
        subset=['wallet_address', 'token_address']
    ).sort_values('fetched_at', ascending=False)
    
    logger.info(f"Found {len(unique_wallets)} unique unfetched wallet-token pairs")
    
    # Return limited batch
    result = unique_wallets.head(batch_limit)
    logger.info(f"Returning {len(result)} wallets for processing")
    return result




def transform_trade_raw(trade: Dict[str, Any], wallet_address: str, 
                       fetched_at: pd.Timestamp) -> Optional[Dict[str, Any]]:
    """
    Transform raw API trade data to raw bronze schema (new approach)
    Stores complete swap data without interpretation or filtering
    """
    tx_hash = trade.get('tx_hash', '')
    if not tx_hash:
        return None
    
    # Validate that this transaction belongs to our target wallet
    if trade.get('owner') != wallet_address:
        return None
    
    # Get base and quote token info from API response
    base = trade.get('base', {})
    quote = trade.get('quote', {})
    
    # Create timestamp from block_unix_time
    trade_timestamp = fetched_at
    if trade.get('block_unix_time'):
        try:
            trade_timestamp = pd.Timestamp(trade.get('block_unix_time'), unit='s', tz='UTC')
        except (ValueError, TypeError):
            pass
    
    # Store RAW data without any interpretation
    return {
        # Core identification
        "wallet_address": wallet_address,
        "transaction_hash": tx_hash,
        "timestamp": trade_timestamp,
        
        # Raw API response - Base token (preserve all fields as-is)
        "base_symbol": base.get('symbol'),
        "base_address": base.get('address'),
        "base_type_swap": base.get('type_swap'),  # "from" or "to" - no interpretation
        "base_ui_change_amount": base.get('ui_change_amount'),  # Keep negative/positive as-is
        "base_nearest_price": base.get('nearest_price'),
        "base_decimals": base.get('decimals'),
        "base_ui_amount": base.get('ui_amount'),
        "base_change_amount": str(base.get('change_amount', '')),  # Raw amount as string
        
        # Raw API response - Quote token (preserve all fields as-is)
        "quote_symbol": quote.get('symbol'),
        "quote_address": quote.get('address'),
        "quote_type_swap": quote.get('type_swap'),  # "from" or "to" - no interpretation
        "quote_ui_change_amount": quote.get('ui_change_amount'),  # Keep negative/positive as-is
        "quote_nearest_price": quote.get('nearest_price'),
        "quote_decimals": quote.get('decimals'),
        "quote_ui_amount": quote.get('ui_amount'),
        "quote_change_amount": str(quote.get('change_amount', '')),  # Raw amount as string
        
        # Transaction metadata (preserve as-is)
        "source": trade.get('source'),
        "tx_type": trade.get('tx_type'),
        "block_unix_time": trade.get('block_unix_time'),
        "owner": trade.get('owner'),  # Store owner field from API
        
        # Processing state tracking (defaults for new records)
        "processed_for_pnl": False,  # Will be processed by silver layer
        "pnl_processed_at": None,
        "pnl_processing_batch_id": None,
        
        # Metadata
        "fetched_at": fetched_at,
        "data_source": "birdeye_v3"
    }


def fetch_bronze_wallet_transactions(**context):
    """
    Fetch transaction history for whale wallets and store as RAW data (uses raw approach)
    
    Extracted from bronze_wallet_transactions_dag.py - now using raw data storage
    """
    logger = logging.getLogger(__name__)
    
    # Generate batch ID
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fetched_at = pd.Timestamp.utcnow()
    
    logger.info(f"Starting raw bronze wallet transactions fetch - batch {batch_id}")
    
    # Read whale data (but don't filter by token)
    whales_df = read_unfetched_whales(BRONZE_WALLET_BATCH_LIMIT)
    
    if whales_df.empty:
        logger.info("No unfetched whales found")
        return {"wallets_processed": 0, "transactions_fetched": 0, "errors": 0}
    
    # Get unique wallets (removing token_address constraint)
    unique_wallets = whales_df['wallet_address'].drop_duplicates()
    logger.info(f"Processing {len(unique_wallets)} unique wallet addresses")
    
    # Initialize BirdEye client
    try:
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
        
        birdeye_client = BirdEyeAPIClient(api_key)
    except Exception as e:
        logger.error(f"Failed to initialize BirdEye client: {e}")
        raise
    
    # Initialize MinIO client
    s3_client = get_minio_client()
    
    all_transactions = []
    wallets_processed = 0
    errors = 0
    
    # Process each unique wallet (no token filtering)
    for wallet_address in unique_wallets:
        logger.info(f"Fetching transactions for wallet {wallet_address[:10]}...")
        
        try:
            # Fetch wallet transactions from BirdEye API
            response = birdeye_client.get_wallet_transactions(
                wallet_address=wallet_address,
                limit=MAX_TRANSACTIONS_PER_WALLET
            )
            
            # Parse response
            trades = []
            if response.get('success') and 'data' in response:
                # Handle different response formats
                if isinstance(response['data'], list):
                    trades = response['data']
                elif isinstance(response['data'], dict):
                    trades = response['data'].get('items', response['data'].get('trades', []))
            
        except Exception as e:
            logger.error(f"BirdEye API error for wallet {wallet_address[:10]}...: {e}")
            errors += 1
            trades = []
        
        # Transform trades using RAW transformation (no token filtering)
        for trade in trades:
            transformed = transform_trade_raw(trade, wallet_address, fetched_at)
            if transformed:
                transformed['batch_id'] = batch_id
                all_transactions.append(transformed)
        
        wallets_processed += 1
        logger.info(f"Fetched {len(trades)} transactions for wallet {wallet_address[:10]}...")
        
        # Rate limiting
        if wallet_address != unique_wallets.iloc[-1]:  # Don't sleep after last wallet
            time.sleep(WALLET_API_DELAY)
    
    # Convert to DataFrame and save to MinIO
    if all_transactions:
        logger.info(f"Processing {len(all_transactions)} total raw transactions")
        
        # Create DataFrame
        transactions_df = pd.DataFrame(all_transactions)
        
        # Ensure all columns from raw schema exist
        schema = get_raw_transaction_schema()
        for field in schema:
            if field.name not in transactions_df.columns:
                transactions_df[field.name] = None
        
        # Select only columns in schema and reorder
        transactions_df = transactions_df[[field.name for field in schema]]
        
        # Convert to PyArrow table with schema
        table = pa.Table.from_pandas(transactions_df, schema=schema)
        
        # Prepare MinIO path for RAW data
        date_partition = datetime.utcnow().strftime("date=%Y-%m-%d")
        file_name = f"wallet_transactions_{batch_id}.parquet"
        s3_key = f"bronze/wallet_transactions/{date_partition}/{file_name}"
        
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
            logger.info(f"Successfully wrote {len(transactions_df)} raw transactions to s3://solana-data/{s3_key}")
            
            # Write success marker
            success_key = f"bronze/wallet_transactions/{date_partition}/_SUCCESS"
            s3_client.put_object(
                Bucket='solana-data',
                Key=success_key,
                Body=b''
            )
            
            # Write processing status file
            status_data = {
                "batch_id": batch_id,
                "processed_wallets": list(unique_wallets),
                "total_transactions": len(all_transactions),
                "timestamp": fetched_at.isoformat(),
                "schema_version": "raw_v1"
            }
            status_key = f"bronze/wallet_transactions/{date_partition}/status_{batch_id}.json"
            s3_client.put_object(
                Bucket='solana-data',
                Key=status_key,
                Body=json.dumps(status_data)
            )
            
        except Exception as e:
            logger.error(f"Error writing raw data to MinIO: {e}")
            raise
    else:
        logger.warning("No transactions fetched")
    
    # Return metadata
    return {
        "wallets_processed": wallets_processed,
        "transactions_fetched": len(all_transactions),
        "errors": errors,
        "s3_path": f"s3://solana-data/{s3_key}" if all_transactions else None,
        "batch_id": batch_id,
        "total_transactions_saved": len(all_transactions)
    }
