"""
Test DAG for bronze layer MinIO integration
"""
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def test_minio_write(**context):
    """Test writing sample data to MinIO bronze layer"""
    import logging
    logger = logging.getLogger(__name__)
    
    # Create test data
    test_data = {
        'token_address': ['TEST123', 'TEST456', 'TEST789'],
        'symbol': ['TST1', 'TST2', 'TST3'],
        'name': ['Test Token 1', 'Test Token 2', 'Test Token 3'],
        'liquidity': [250000.0, 500000.0, 750000.0],
        'price': [0.001, 0.002, 0.003],
        'market_cap': [1000000.0, 2000000.0, 3000000.0],
        'volume_24h_usd': [300000.0, 400000.0, 500000.0],
        'price_change_24h_percent': [15.5, 25.5, 35.5],
        'ingested_at': pd.Timestamp.utcnow(),
        'batch_id': datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    }
    
    df = pd.DataFrame(test_data)
    logger.info(f"Created test dataframe with {len(df)} rows")
    
    # Convert to PyArrow table
    table = pa.Table.from_pandas(df)
    
    # Write to buffer
    buffer = BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    
    # Initialize MinIO client
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4')
    )
    
    # Prepare S3 path
    date_partition = datetime.utcnow().strftime("date=%Y-%m-%d")
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_name = f"test_token_list_{batch_id}.parquet"
    s3_key = f"bronze/token_list_v3/{date_partition}/{file_name}"
    
    # Write to MinIO
    try:
        s3_client.put_object(
            Bucket='solana-data',
            Key=s3_key,
            Body=buffer.getvalue()
        )
        logger.info(f"Successfully wrote test data to s3://solana-data/{s3_key}")
        
        # Write success marker
        success_key = f"bronze/token_list_v3/{date_partition}/_SUCCESS"
        s3_client.put_object(
            Bucket='solana-data',
            Key=success_key,
            Body=b''
        )
        
        return {
            'status': 'success',
            'rows_written': len(df),
            's3_path': f"s3://solana-data/{s3_key}",
            'batch_id': batch_id
        }
        
    except Exception as e:
        logger.error(f"Error writing to MinIO: {e}")
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
    'test_bronze_layer',
    default_args=default_args,
    description='Test DAG for bronze layer MinIO integration',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'bronze'],
)

test_task = PythonOperator(
    task_id='test_minio_write',
    python_callable=test_minio_write,
    provide_context=True,
    dag=dag,
)