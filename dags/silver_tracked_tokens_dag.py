"""
Silver Tracked Tokens DAG
Transforms bronze token list data into filtered silver layer tracked tokens
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
import subprocess
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago


# Configuration
class FilterConfig:
    """Configuration for token filtering criteria"""
    limit: int = 50
    min_liquidity: float = 10000
    min_volume: float = 50000
    min_volume_mcap_ratio: float = 0.05
    min_price_change: float = 30


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4')
    )


def get_silver_schema() -> pa.Schema:
    """Define PyArrow schema for silver tracked tokens"""
    return pa.schema([
        # Basic token info
        pa.field("token_address", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("decimals", pa.int32(), nullable=True),
        
        # Mapped field names to match original schema
        pa.field("logoURI", pa.string(), nullable=True),
        pa.field("volume24hUSD", pa.float64(), nullable=True),
        pa.field("volume24hChangePercent", pa.float64(), nullable=True),
        pa.field("price24hChangePercent", pa.float64(), nullable=True),
        pa.field("marketcap", pa.float64(), nullable=True),
        
        # Core metrics
        pa.field("liquidity", pa.float64(), nullable=True),
        pa.field("price", pa.float64(), nullable=True),
        pa.field("fdv", pa.float64(), nullable=True),
        pa.field("rank", pa.int32(), nullable=True),
        
        # Calculated fields
        pa.field("volume_mcap_ratio", pa.float64(), nullable=True),
        pa.field("quality_score", pa.float64(), nullable=True),
        
        # Reference and metadata
        pa.field("bronze_id", pa.string(), nullable=True),
        pa.field("created_at", pa.timestamp('us'), nullable=False),
        pa.field("updated_at", pa.timestamp('us'), nullable=False),
        pa.field("processing_date", pa.date32(), nullable=False),
    ])


def transform_tracked_tokens(**context):
    """
    Transform bronze token list data into silver tracked tokens using DuckDB container
    """
    logger = logging.getLogger(__name__)
    
    # Get configuration
    config = FilterConfig()
    
    try:
        # Step 1: Query bronze data using DuckDB container
        logger.info("Querying bronze token data via DuckDB container")
        
        # Create Python script for DuckDB container execution
        duckdb_script = f'''
import duckdb
import json
import sys

# Connect to DuckDB
conn = duckdb.connect("/data/analytics.duckdb")

try:
    # Configure S3/MinIO access
    conn.execute("LOAD httpfs;")
    conn.execute("SET s3_endpoint='minio:9000';")
    conn.execute("SET s3_access_key_id='minioadmin';")
    conn.execute("SET s3_secret_access_key='minioadmin123';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    # First check what bronze data is available
    bronze_query = """
    SELECT COUNT(*) as total_tokens
    FROM read_parquet('s3://solana-data/bronze/token_list_v3/**/*.parquet')
    """
    
    result = conn.execute(bronze_query).fetchone()
    total_bronze_tokens = result[0] if result else 0
    
    if total_bronze_tokens == 0:
        print("NO_BRONZE_DATA")
        sys.exit(0)
    
    # Main filtering query
    filter_query = """
    WITH bronze_tokens AS (
        SELECT 
            token_address,
            symbol,
            name,
            decimals,
            logo_uri,
            liquidity,
            price,
            fdv,
            market_cap,
            volume_24h_usd,
            volume_24h_change_percent,
            price_change_1h_percent,
            price_change_2h_percent,
            price_change_4h_percent,
            price_change_8h_percent,
            price_change_24h_percent,
            ingested_at,
            batch_id,
            -- Add row number to handle duplicates
            ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ingested_at DESC) as rn
        FROM read_parquet('s3://solana-data/bronze/token_list_v3/**/*.parquet')
        WHERE token_address IS NOT NULL
    ),
    filtered_tokens AS (
        SELECT *
        FROM bronze_tokens
        WHERE rn = 1  -- Keep only latest version of each token
          AND logo_uri IS NOT NULL 
          AND logo_uri != ''
          AND liquidity >= {config.min_liquidity}
          AND volume_24h_usd >= {config.min_volume}
          AND price_change_24h_percent >= {config.min_price_change}
          -- All price changes must be positive
          AND price_change_1h_percent > 0
          AND price_change_2h_percent > 0
          AND price_change_4h_percent > 0
          AND price_change_8h_percent > 0
          AND price_change_24h_percent > 0
    )
    SELECT 
        token_address,
        symbol,
        name,
        decimals,
        logo_uri as logoURI,
        liquidity,
        price,
        fdv,
        market_cap as marketcap,
        volume_24h_usd as volume24hUSD,
        volume_24h_change_percent as volume24hChangePercent,
        price_change_24h_percent as price24hChangePercent,
        -- Calculate volume/mcap ratio
        CASE 
            WHEN market_cap > 0 THEN volume_24h_usd / market_cap 
            ELSE NULL 
        END as volume_mcap_ratio,
        batch_id as bronze_id,
        ingested_at
    FROM filtered_tokens
    WHERE (market_cap IS NULL OR market_cap <= 0 OR 
           (volume_24h_usd / market_cap) >= {config.min_volume_mcap_ratio})
    ORDER BY liquidity DESC
    LIMIT {config.limit}
    """
    
    # Execute query and return results as JSON
    result = conn.execute(filter_query).fetchall()
    columns = [desc[0] for desc in conn.description]
    
    # Output results
    print("QUERY_RESULTS_START")
    print(json.dumps({{"total_bronze_tokens": total_bronze_tokens}}))
    print("COLUMNS_START")
    print(json.dumps(columns))
    print("COLUMNS_END")
    print("DATA_START")
    for row in result:
        print(json.dumps(list(row)))
    print("DATA_END")
    print("QUERY_RESULTS_END")
    
except Exception as e:
    print(f"ERROR: {{e}}")
    sys.exit(1)
finally:
    conn.close()
'''
        
        # Execute DuckDB script in container
        cmd = ['docker', 'exec', 'claude_pipeline-duckdb', 'python3', '-c', duckdb_script]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            logger.error(f"DuckDB query failed with return code {result.returncode}")
            logger.error(f"STDERR: {result.stderr}")
            logger.error(f"STDOUT: {result.stdout}")
            raise Exception(f"DuckDB query failed: {result.stderr}")
        
        # Parse results
        output_lines = result.stdout.split('\\n')
        
        if "NO_BRONZE_DATA" in output_lines:
            logger.warning("No bronze data found - skipping transformation")
            return {{"tokens_processed": 0, "tokens_tracked": 0}}
        
        # Parse query results
        in_results = False
        in_columns = False
        in_data = False
        columns = []
        data_rows = []
        total_bronze_tokens = 0
        
        for line in output_lines:
            if line == "QUERY_RESULTS_START":
                in_results = True
                continue
            elif line == "QUERY_RESULTS_END":
                break
            elif not in_results:
                continue
            
            if line.startswith('{{"total_bronze_tokens"'):
                metadata = json.loads(line)
                total_bronze_tokens = metadata["total_bronze_tokens"]
            elif line == "COLUMNS_START":
                in_columns = True
                continue
            elif line == "COLUMNS_END":
                in_columns = False
                continue
            elif line == "DATA_START":
                in_data = True
                continue
            elif line == "DATA_END":
                in_data = False
                continue
            elif in_columns and line.strip():
                columns = json.loads(line)
            elif in_data and line.strip():
                data_rows.append(json.loads(line))
        
        logger.info(f"Found {{total_bronze_tokens}} tokens in bronze, {{len(data_rows)}} passed filters")
        
        if not data_rows:
            logger.warning("No tokens passed filtering criteria")
            return {{"tokens_processed": total_bronze_tokens, "tokens_tracked": 0}}
        
        # Convert to DataFrame
        df = pd.DataFrame(data_rows, columns=columns)
        
        # Add calculated fields and metadata
        current_time = pd.Timestamp.utcnow()
        processing_date = current_time.date()
        
        df['rank'] = None  # No rank calculation for now
        df['quality_score'] = None  # No quality score calculation for now
        df['created_at'] = current_time
        df['updated_at'] = current_time
        df['processing_date'] = processing_date
        
        # Ensure schema compliance
        schema = get_silver_schema()
        
        # Reorder columns to match schema
        ordered_columns = [field.name for field in schema]
        df = df[ordered_columns]
        
        logger.info(f"Sample transformed data:\\n{{df.head(3)[['token_address', 'symbol', 'volume24hUSD', 'volume_mcap_ratio']]}}")
        
        # Convert to PyArrow table with schema
        table = pa.Table.from_pandas(df, schema=schema)
        
        # Prepare MinIO path with date partitioning
        date_partition = processing_date.strftime("processing_date=%Y-%m-%d")
        timestamp = current_time.strftime("%Y%m%d_%H%M%S")
        file_name = f"tracked_tokens_{{timestamp}}.parquet"
        s3_key = f"silver/tracked_tokens/{{date_partition}}/{{file_name}}"
        
        # Write to MinIO
        s3_client = get_minio_client()
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        try:
            s3_client.put_object(
                Bucket='solana-data',
                Key=s3_key,
                Body=buffer.getvalue()
            )
            logger.info(f"Successfully wrote {{len(df)}} tracked tokens to s3://solana-data/{{s3_key}}")
            
            # Write success marker
            success_key = f"silver/tracked_tokens/{{date_partition}}/_SUCCESS"
            s3_client.put_object(
                Bucket='solana-data',
                Key=success_key,
                Body=b''
            )
            
        except Exception as e:
            logger.error(f"Error writing to MinIO: {{e}}")
            raise
        
        # Return metadata
        return {{
            "tokens_processed": total_bronze_tokens,
            "tokens_tracked": len(df),
            "s3_path": f"s3://solana-data/{{s3_key}}",
            "processing_date": str(processing_date)
        }}
        
    except Exception as e:
        logger.error(f"Silver transformation failed: {{e}}")
        raise


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
    'silver_tracked_tokens',
    default_args=default_args,
    description='Transform bronze token list into silver tracked tokens',
    schedule_interval='@daily',  # Run daily after bronze layer
    catchup=False,
    tags=['silver', 'tokens', 'transformation'],
)

# Define task
transform_task = PythonOperator(
    task_id='transform_tracked_tokens',
    python_callable=transform_tracked_tokens,
    provide_context=True,
    dag=dag,
)

# This DAG can be triggered manually or scheduled to run after bronze layer completion