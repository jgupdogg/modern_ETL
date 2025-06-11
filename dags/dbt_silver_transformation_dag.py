#!/usr/bin/env python3
"""
Airflow DAG: DBT Silver Layer Transformations
Orchestrates bronze to silver data transformations using DBT models.
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
DAG_ID = "dbt_silver_transformation"
SCHEDULE_INTERVAL = "*/10 * * * *"  # Every 10 minutes

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DBT silver layer transformations from bronze webhook data',
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'silver', 'transformations', 'medallion-architecture']
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

# DBT Commands using PythonOperator
dbt_run_bronze = PythonOperator(
    task_id='dbt_run_bronze_models',
    python_callable=run_dbt_command,
    op_args=['dbt run --models bronze'],
    dag=dag,
    retries=2
)

dbt_test_bronze = PythonOperator(
    task_id='dbt_test_bronze_models',
    python_callable=run_dbt_command,
    op_args=['dbt test --models bronze'],
    dag=dag
)

dbt_run_silver = PythonOperator(
    task_id='dbt_run_silver_models',
    python_callable=run_dbt_command,
    op_args=['dbt run --models silver'],
    dag=dag,
    retries=2
)

dbt_test_silver = PythonOperator(
    task_id='dbt_test_silver_models',
    python_callable=run_dbt_command,
    op_args=['dbt test --models silver'],
    dag=dag
)

dbt_run_all_tests = PythonOperator(
    task_id='dbt_run_all_tests',
    python_callable=run_dbt_command,
    op_args=['dbt test'],
    dag=dag
)

@task(dag=dag)
def validate_silver_output(**context) -> Dict[str, Any]:
    """Validate silver layer output and data quality."""
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
    # Check silver tables exist and have data
    events = conn.execute('SELECT COUNT(*) FROM silver_webhook_events;').fetchone()[0]
    transactions = conn.execute('SELECT COUNT(*) FROM silver_transaction_details;').fetchone()[0] 
    quality = conn.execute('SELECT COUNT(*) FROM silver_data_quality_metrics;').fetchone()[0]
    
    print(f'VALIDATION_RESULT:{{\\\"events\\\":{events},\\\"transactions\\\":{transactions},\\\"quality\\\":{quality}}}')
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
            'silver_events': validation_data['events'],
            'silver_transactions': validation_data['transactions'],
            'quality_metrics': validation_data['quality'],
            'validation_timestamp': context['ts']
        }
    else:
        raise ValueError(f"Validation failed: {result.stdout}")

# Define task dependencies
with dag:
    # Sequential bronze → silver flow
    dbt_run_bronze >> dbt_test_bronze >> dbt_run_silver >> dbt_test_silver >> dbt_run_all_tests >> validate_silver_output()