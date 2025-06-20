#!/usr/bin/env python3
"""
Test DAG to verify Kafka connectivity and basic data flow without PySpark.
This tests the core workflow without the Java/Spark dependency.
"""

from datetime import datetime, timedelta
from typing import Dict, Any
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# DAG Configuration
DAG_ID = "test_kafka_pipeline"
SCHEDULE_INTERVAL = "*/3 * * * *"  # Every 3 minutes

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Test pipeline to verify Kafka connectivity without PySpark',
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['test', 'kafka', 'redpanda']
)

def get_config() -> Dict[str, Any]:
    """Get configuration from Airflow Variables with defaults."""
    return {
        'redpanda_brokers': Variable.get('REDPANDA_BROKERS', 'localhost:19092'),
        'webhook_topic': Variable.get('WEBHOOK_TOPIC', 'webhooks'),
        'minio_endpoint': Variable.get('MINIO_ENDPOINT', 'http://localhost:9000'),
        'minio_access_key': Variable.get('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': Variable.get('MINIO_SECRET_KEY', 'minioadmin123'),
        'minio_bucket': Variable.get('MINIO_BUCKET', 'webhook-data'),
    }

@task(dag=dag)
def test_kafka_consumer(**context) -> Dict[str, Any]:
    """Test reading from Kafka/Redpanda using kafka-python."""
    import json
    from kafka import KafkaConsumer
    from datetime import datetime
    
    config = get_config()
    
    print(f"=== Kafka Consumer Test ===")
    print(f"Brokers: {config['redpanda_brokers']}")
    print(f"Topic: {config['webhook_topic']}")
    
    try:
        # Create consumer with short timeout for testing
        consumer = KafkaConsumer(
            config['webhook_topic'],
            bootstrap_servers=config['redpanda_brokers'].split(','),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=10000  # 10 second timeout
        )
        
        messages = []
        message_count = 0
        
        print("Consuming messages for 10 seconds...")
        
        for message in consumer:
            if message.value:
                try:
                    data = json.loads(message.value)
                    messages.append({
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': message.timestamp,
                        'key': message.key.decode('utf-8') if message.key else None,
                        'value': data
                    })
                    message_count += 1
                    print(f"  Message {message_count}: {data}")
                    
                    # Limit to 10 messages for testing
                    if message_count >= 10:
                        break
                except json.JSONDecodeError as e:
                    print(f"  Invalid JSON in message: {e}")
                except Exception as e:
                    print(f"  Error processing message: {e}")
        
        consumer.close()
        
        print(f"✅ Successfully consumed {message_count} messages from Kafka")
        
        return {
            'status': 'success',
            'messages_consumed': message_count,
            'sample_messages': messages[:3],  # First 3 messages as sample
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Error consuming from Kafka: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'messages_consumed': 0,
            'timestamp': datetime.now().isoformat()
        }

@task(dag=dag)
def test_minio_upload(kafka_result: Dict[str, Any], **context) -> Dict[str, Any]:
    """Test uploading data to MinIO."""
    import json
    import boto3
    from botocore.client import Config
    from datetime import datetime
    
    config = get_config()
    
    print(f"=== MinIO Upload Test ===")
    print(f"Endpoint: {config['minio_endpoint']}")
    print(f"Bucket: {config['minio_bucket']}")
    
    if kafka_result['messages_consumed'] == 0:
        print("No messages to upload")
        return {
            'status': 'skipped',
            'reason': 'No messages from Kafka',
            'timestamp': datetime.now().isoformat()
        }
    
    try:
        # Create S3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=config['minio_endpoint'],
            aws_access_key_id=config['minio_access_key'],
            aws_secret_access_key=config['minio_secret_key'],
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # Create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=config['minio_bucket'])
            print(f"Bucket '{config['minio_bucket']}' exists")
        except:
            s3_client.create_bucket(Bucket=config['minio_bucket'])
            print(f"Created bucket '{config['minio_bucket']}'")
        
        # Create a test file with the Kafka results
        file_content = {
            'test_run': {
                'timestamp': datetime.now().isoformat(),
                'kafka_results': kafka_result,
                'processed_by': 'airflow_test_dag'
            }
        }
        
        file_key = f"test-runs/{datetime.now().strftime('%Y/%m/%d/%H%M%S')}.json"
        
        # Upload to MinIO
        s3_client.put_object(
            Bucket=config['minio_bucket'],
            Key=file_key,
            Body=json.dumps(file_content, indent=2),
            ContentType='application/json'
        )
        
        print(f"✅ Successfully uploaded test file to MinIO: {file_key}")
        
        # Verify by listing objects
        response = s3_client.list_objects_v2(
            Bucket=config['minio_bucket'],
            Prefix='test-runs/'
        )
        
        object_count = response.get('KeyCount', 0)
        print(f"Total test files in bucket: {object_count}")
        
        return {
            'status': 'success',
            'file_key': file_key,
            'bucket': config['minio_bucket'],
            'kafka_messages_processed': kafka_result['messages_consumed'],
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Error uploading to MinIO: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

@task(dag=dag)
def test_summary(minio_result: Dict[str, Any], **context) -> Dict[str, Any]:
    """Summarize the test results."""
    print(f"=== Test Summary ===")
    
    if minio_result['status'] == 'success':
        print(f"✅ End-to-end test PASSED")
        print(f"  - Kafka messages: {minio_result['kafka_messages_processed']}")
        print(f"  - MinIO file: {minio_result['file_key']}")
        status = 'PASSED'
    else:
        print(f"❌ End-to-end test FAILED")
        print(f"  - Error: {minio_result.get('error', 'Unknown error')}")
        status = 'FAILED'
    
    return {
        'test_status': status,
        'final_result': minio_result,
        'timestamp': datetime.now().isoformat()
    }

# Define task dependencies
with dag:
    kafka_task = test_kafka_consumer()
    minio_task = test_minio_upload(kafka_task)
    summary_task = test_summary(minio_task)
    
    kafka_task >> minio_task >> summary_task