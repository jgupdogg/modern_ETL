#!/usr/bin/env python3
"""
Airflow DAG: DBT Token Transformations
Orchestrates bronze to silver token data transformations using DBT models.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import subprocess
import os

# DAG Configuration
DAG_ID = "dbt_token_transformation"
SCHEDULE_INTERVAL = "@daily"  # Run daily after bronze token ingestion

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DBT token transformations from bronze to silver layer',
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'tokens', 'silver', 'transformations', 'medallion-architecture']
)

# Function to run DBT commands
def run_dbt_command(command):
    """Execute DBT command with proper configuration."""
    env = os.environ.copy()
    env['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
    
    result = subprocess.run(
        command.split(),
        cwd='/opt/airflow/dbt',
        env=env,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"DBT command failed: {result.stderr}")
    
    print(result.stdout)
    return result.stdout

# DBT Commands for token models
dbt_run_bronze_tokens = PythonOperator(
    task_id='dbt_run_bronze_tokens',
    python_callable=run_dbt_command,
    op_args=['dbt run --models bronze_tokens'],
    dag=dag,
    retries=2
)

dbt_test_bronze_tokens = PythonOperator(
    task_id='dbt_test_bronze_tokens',
    python_callable=run_dbt_command,
    op_args=['dbt test --models bronze_tokens'],
    dag=dag
)

dbt_run_silver_tokens = PythonOperator(
    task_id='dbt_run_silver_tokens',
    python_callable=run_dbt_command,
    op_args=['dbt run --models silver_tracked_tokens'],
    dag=dag,
    retries=2
)

dbt_test_silver_tokens = PythonOperator(
    task_id='dbt_test_silver_tokens',
    python_callable=run_dbt_command,
    op_args=['dbt test --models silver_tracked_tokens'],
    dag=dag
)

@task(dag=dag)
def validate_token_output(**context) -> Dict[str, Any]:
    """Validate silver token layer output and data quality."""
    import subprocess
    import json
    
    # Run validation query via DuckDB
    validation_cmd = """
    docker exec claude_pipeline-duckdb python3 -c "
import duckdb
conn = duckdb.connect('/data/analytics.duckdb')
conn.execute('LOAD httpfs;')
conn.execute('SET s3_endpoint=\\'minio:9000\\';')
conn.execute('SET s3_access_key_id=\\'minioadmin\\';')
conn.execute('SET s3_secret_access_key=\\'minioadmin123\\';')
conn.execute('SET s3_use_ssl=false;')
conn.execute('SET s3_url_style=\\'path\\';')

try:
    # Check bronze tokens exist and have data
    bronze_count = conn.execute('SELECT COUNT(*) FROM bronze_tokens;').fetchone()[0]
    bronze_unprocessed = conn.execute('SELECT COUNT(*) FROM bronze_tokens WHERE processing_status = \\\"unprocessed\\\";').fetchone()[0]
    bronze_processed = conn.execute('SELECT COUNT(*) FROM bronze_tokens WHERE processing_status = \\\"silver_processed\\\";').fetchone()[0]
    
    # Check silver tracked tokens 
    silver_count = conn.execute('SELECT COUNT(*) FROM silver_tracked_tokens;').fetchone()[0]
    silver_pending_whales = conn.execute('SELECT COUNT(*) FROM silver_tracked_tokens WHERE whale_fetch_status = \\\"pending\\\";').fetchone()[0]
    silver_newly_tracked = conn.execute('SELECT COUNT(*) FROM silver_tracked_tokens WHERE is_newly_tracked = true;').fetchone()[0]
    
    # Quality metrics
    avg_liquidity = conn.execute('SELECT AVG(liquidity) FROM silver_tracked_tokens;').fetchone()[0]
    avg_volume = conn.execute('SELECT AVG(volume24hUSD) FROM silver_tracked_tokens;').fetchone()[0]
    avg_quality_score = conn.execute('SELECT AVG(quality_score) FROM silver_tracked_tokens;').fetchone()[0]
    
    # Top token by liquidity
    top_token = conn.execute('SELECT symbol, liquidity FROM silver_tracked_tokens ORDER BY liquidity DESC LIMIT 1;').fetchone()
    
    print(f'VALIDATION_RESULT:{{\\\"bronze_tokens\\\":{bronze_count},\\\"bronze_unprocessed\\\":{bronze_unprocessed},\\\"bronze_processed\\\":{bronze_processed},\\\"silver_tracked_tokens\\\":{silver_count},\\\"silver_pending_whales\\\":{silver_pending_whales},\\\"silver_newly_tracked\\\":{silver_newly_tracked},\\\"avg_liquidity\\\":{avg_liquidity or 0},\\\"avg_volume\\\":{avg_volume or 0},\\\"avg_quality_score\\\":{avg_quality_score or 0},\\\"top_token_symbol\\\":\\\"{top_token[0] if top_token else \\\"N/A\\\"}\\\",\\\"top_token_liquidity\\\":{top_token[1] if top_token else 0}}}')
except Exception as e:
    print(f'VALIDATION_ERROR:{e}')
conn.close()
"
    """
    
    result = subprocess.run(validation_cmd, shell=True, capture_output=True, text=True)
    
    # Parse validation results
    if "VALIDATION_RESULT:" in result.stdout:
        result_json = result.stdout.split("VALIDATION_RESULT:")[1].strip()
        validation_data = json.loads(result_json)
        
        return {
            'status': 'success',
            'bronze_tokens': validation_data['bronze_tokens'],
            'bronze_unprocessed': validation_data['bronze_unprocessed'],
            'bronze_processed': validation_data['bronze_processed'],
            'silver_tracked_tokens': validation_data['silver_tracked_tokens'],
            'silver_pending_whales': validation_data['silver_pending_whales'],
            'silver_newly_tracked': validation_data['silver_newly_tracked'],
            'avg_liquidity': validation_data['avg_liquidity'],
            'avg_volume': validation_data['avg_volume'],
            'avg_quality_score': validation_data['avg_quality_score'],
            'top_token_symbol': validation_data['top_token_symbol'],
            'top_token_liquidity': validation_data['top_token_liquidity'],
            'validation_timestamp': context['ts']
        }
    else:
        raise ValueError(f"Token validation failed: {result.stdout}")

# Define task dependencies
with dag:
    # Sequential bronze → silver flow for tokens
    dbt_run_bronze_tokens >> dbt_test_bronze_tokens >> dbt_run_silver_tokens >> dbt_test_silver_tokens >> validate_token_output()