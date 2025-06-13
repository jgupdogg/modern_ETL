"""
Simple Airflow DAG to update Helius webhook with addresses from gold.top_traders.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    'webhook_update_simple',
    default_args=default_args,
    description='Update Helius webhook with gold layer addresses',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test']
)

# Task to update webhook
update_webhook = BashOperator(
    task_id='update_webhook',
    bash_command='''
    cd /home/jgupdogg/dev/claude_pipeline/scripts && \
    source venv/bin/activate && \
    python update_webhook_with_gold_layer.py
    ''',
    dag=dag,
)