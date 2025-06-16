#!/usr/bin/env python3
"""
Test script to trigger and monitor the PySpark streaming DAG.
This script will:
1. Send test data to Redpanda
2. Trigger the DAG manually
3. Monitor execution status
4. Verify results
"""

import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
import os

# Configuration
WEBHOOK_URL = "http://localhost:8000/webhooks"
AIRFLOW_API_URL = "http://localhost:8080/api/v2"
DAG_ID = "pyspark_streaming_pipeline"
REDPANDA_BROKERS = "localhost:19092"
WEBHOOK_TOPIC = "webhooks"

# Airflow credentials
AIRFLOW_USER = "airflow"
AIRFLOW_PASSWORD = "airflow"

def send_test_webhooks(num_messages=5):
    """Send test webhook messages to trigger data flow."""
    print(f"📤 Sending {num_messages} test webhook messages...")
    
    for i in range(num_messages):
        test_payload = {
            "message_id": f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}",
            "event_type": "test_event",
            "source": "dag_test_script",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "test_field": f"test_value_{i}",
                "sequence": i,
                "batch_test": True
            }
        }
        
        try:
            response = requests.post(
                WEBHOOK_URL,
                json=test_payload,
                timeout=5
            )
            if response.status_code == 200:
                print(f"  ✅ Sent message {i+1}/{num_messages}")
            else:
                print(f"  ❌ Failed to send message {i+1}: {response.status_code}")
        except Exception as e:
            print(f"  ❌ Error sending message {i+1}: {e}")
        
        time.sleep(0.5)  # Small delay between messages
    
    print(f"✅ Completed sending {num_messages} test messages")

def send_direct_to_redpanda(num_messages=3):
    """Send messages directly to Redpanda/Kafka topic."""
    print(f"📤 Sending {num_messages} messages directly to Redpanda...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[REDPANDA_BROKERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        for i in range(num_messages):
            message_data = {
                "message_id": f"direct_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}",
                "timestamp": datetime.now().isoformat(),
                "source_ip": "127.0.0.1",
                "file_path": f"/test/direct_message_{i}.json",
                "payload": json.dumps({
                    "event_type": "direct_kafka_test",
                    "sequence": i,
                    "test_data": f"direct_value_{i}"
                })
            }
            
            future = producer.send(
                WEBHOOK_TOPIC,
                key=f"test_key_{i}",
                value=message_data
            )
            
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            print(f"  ✅ Sent direct message {i+1}/{num_messages} to partition {record_metadata.partition}")
        
        producer.flush()
        producer.close()
        print(f"✅ Completed sending {num_messages} direct messages to Redpanda")
        
    except Exception as e:
        print(f"❌ Error sending direct messages to Redpanda: {e}")

def trigger_dag():
    """Trigger the DAG manually via Airflow API."""
    print(f"🚀 Triggering DAG: {DAG_ID}")
    
    trigger_url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns"
    
    payload = {
        "dag_run_id": f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "logical_date": datetime.now().isoformat(),
        "conf": {
            "test_run": True,
            "triggered_by": "test_script"
        }
    }
    
    try:
        response = requests.post(
            trigger_url,
            json=payload,
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            result = response.json()
            dag_run_id = result.get('dag_run_id')
            print(f"✅ DAG triggered successfully: {dag_run_id}")
            return dag_run_id
        else:
            print(f"❌ Failed to trigger DAG: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error triggering DAG: {e}")
        return None

def monitor_dag_run(dag_run_id, timeout_minutes=10):
    """Monitor DAG execution status."""
    if not dag_run_id:
        print("❌ No DAG run ID to monitor")
        return False
    
    print(f"👀 Monitoring DAG run: {dag_run_id}")
    
    status_url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}"
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    while time.time() - start_time < timeout_seconds:
        try:
            response = requests.get(
                status_url,
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                timeout=5
            )
            
            if response.status_code == 200:
                dag_run = response.json()
                state = dag_run.get('state')
                
                print(f"  📊 DAG state: {state}")
                
                if state == 'success':
                    print(f"✅ DAG completed successfully!")
                    return True
                elif state == 'failed':
                    print(f"❌ DAG failed!")
                    return False
                elif state in ['running', 'queued']:
                    print(f"  ⏳ DAG still {state}...")
                    time.sleep(10)
                else:
                    print(f"  🤔 Unknown state: {state}")
                    time.sleep(5)
            else:
                print(f"❌ Error checking DAG status: {response.status_code}")
                time.sleep(5)
                
        except Exception as e:
            print(f"❌ Error monitoring DAG: {e}")
            time.sleep(5)
    
    print(f"⏰ Timeout reached ({timeout_minutes} minutes)")
    return False

def check_dag_status():
    """Check if DAG is available and enabled."""
    print(f"🔍 Checking DAG status: {DAG_ID}")
    
    dag_url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}"
    
    try:
        response = requests.get(
            dag_url,
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=5
        )
        
        if response.status_code == 200:
            dag_info = response.json()
            is_paused = dag_info.get('is_paused', True)
            
            print(f"  📋 DAG found - Paused: {is_paused}")
            
            if is_paused:
                print("  ⚠️  DAG is paused. You may need to unpause it in Airflow UI")
                print(f"  🌐 Airflow UI: http://localhost:8080/dags/{DAG_ID}")
            
            return True
        else:
            print(f"❌ DAG not found: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking DAG: {e}")
        return False

def verify_services():
    """Verify that required services are running."""
    print("🔧 Verifying services...")
    
    services = [
        ("Webhook Listener", "http://localhost:8000/health"),
        ("Airflow API", f"{AIRFLOW_API_URL}/monitor/health"),
        ("MinIO", "http://localhost:9000/minio/health/live"),
        ("Redpanda Console", "http://localhost:8090")
    ]
    
    all_good = True
    
    for service_name, url in services:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"  ✅ {service_name}: OK")
            else:
                print(f"  ⚠️  {service_name}: Status {response.status_code}")
                all_good = False
        except Exception as e:
            print(f"  ❌ {service_name}: {e}")
            all_good = False
    
    return all_good

def main():
    """Main test execution."""
    print("🧪 Starting PySpark Streaming DAG Test")
    print("=" * 50)
    
    # Step 1: Verify services
    if not verify_services():
        print("\n❌ Some services are not running. Please check your setup.")
        return
    
    # Step 2: Check DAG status
    if not check_dag_status():
        print("\n❌ DAG is not available. Please check your DAG file.")
        return
    
    # Step 3: Send test data
    print("\n📤 Sending test data...")
    send_test_webhooks(3)
    send_direct_to_redpanda(3)
    
    # Wait a moment for data to settle
    print("\n⏳ Waiting 5 seconds for data to settle...")
    time.sleep(5)
    
    # Step 4: Trigger DAG
    print("\n🚀 Triggering DAG...")
    dag_run_id = trigger_dag()
    
    if not dag_run_id:
        print("\n❌ Failed to trigger DAG")
        return
    
    # Step 5: Monitor execution
    print("\n👀 Monitoring execution...")
    success = monitor_dag_run(dag_run_id, timeout_minutes=10)
    
    # Step 6: Summary
    print("\n" + "=" * 50)
    if success:
        print("🎉 Test completed successfully!")
        print(f"🌐 Check results in Airflow UI: http://localhost:8080/dags/{DAG_ID}")
        print("🗃️  Check MinIO Console: http://localhost:9001")
    else:
        print("❌ Test failed or timed out")
        print(f"🌐 Check logs in Airflow UI: http://localhost:8080/dags/{DAG_ID}")

if __name__ == "__main__":
    # Check if required packages are available
    try:
        import kafka
    except ImportError:
        print("❌ kafka-python package not found. Installing...")
        os.system("pip install kafka-python")
        import kafka
    
    main()