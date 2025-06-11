#!/usr/bin/env python3
"""
Simple test DAG to verify Airflow setup without complex dependencies.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

# Simple DAG configuration
dag = DAG(
    'test_simple_pipeline',
    default_args={
        'owner': 'data-team',
        'depends_on_past': False,
        'start_date': datetime(2025, 6, 4),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Simple test pipeline to verify basic Airflow functionality',
    schedule='*/2 * * * *',  # Every 2 minutes
    catchup=False,
    max_active_runs=1,
    tags=['test', 'simple']
)

@task(dag=dag)
def test_basic_python(**context):
    """Test basic Python functionality."""
    import json
    import os
    
    print("=== Basic Python Test ===")
    print(f"Python version: {os.sys.version}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Environment variables:")
    for key in ['AIRFLOW_HOME', 'PATH']:
        print(f"  {key}: {os.environ.get(key, 'Not set')}")
    
    return {
        'status': 'success',
        'message': 'Basic Python test completed',
        'timestamp': datetime.now().isoformat()
    }

@task(dag=dag)
def test_airflow_variables(**context):
    """Test Airflow Variable access."""
    from airflow.models import Variable
    
    print("=== Airflow Variables Test ===")
    
    # Test accessing our variables
    variables_to_check = [
        'REDPANDA_BROKERS',
        'WEBHOOK_TOPIC', 
        'MINIO_ENDPOINT',
        'LOCAL_DATA_PATH'
    ]
    
    results = {}
    for var_name in variables_to_check:
        try:
            value = Variable.get(var_name, default_var=None)
            results[var_name] = value if value else 'Not set'
            print(f"  {var_name}: {results[var_name]}")
        except Exception as e:
            results[var_name] = f"Error: {str(e)}"
            print(f"  {var_name}: ERROR - {str(e)}")
    
    return {
        'status': 'success',
        'variables': results,
        'timestamp': datetime.now().isoformat()
    }

@task(dag=dag)
def test_network_connectivity(basic_result, **context):
    """Test network connectivity to our services."""
    import urllib.request
    import json
    
    print("=== Network Connectivity Test ===")
    
    services_to_test = [
        ('Webhook Listener', 'http://localhost:8000/health'),
        ('MinIO Health', 'http://localhost:9000/minio/health/live'),
        ('Redpanda Console', 'http://localhost:8090'),
    ]
    
    results = {}
    for service_name, url in services_to_test:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Airflow-Test'})
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    results[service_name] = 'OK'
                    print(f"  ✅ {service_name}: OK")
                else:
                    results[service_name] = f"Status: {response.status}"
                    print(f"  ⚠️  {service_name}: Status {response.status}")
        except Exception as e:
            results[service_name] = f"Error: {str(e)}"
            print(f"  ❌ {service_name}: {str(e)}")
    
    return {
        'status': 'success',
        'connectivity': results,
        'basic_test_result': basic_result,
        'timestamp': datetime.now().isoformat()
    }

@task(dag=dag)
def test_file_operations(connectivity_result, **context):
    """Test file system operations."""
    import os
    import tempfile
    
    print("=== File Operations Test ===")
    
    results = {}
    
    # Test temporary file creation
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write('Test data from Airflow task')
        
        # Read it back
        with open(temp_path, 'r') as temp_file:
            content = temp_file.read()
        
        os.unlink(temp_path)  # Clean up
        
        results['temp_file_test'] = 'OK'
        print(f"  ✅ Temp file operations: OK")
        
    except Exception as e:
        results['temp_file_test'] = f"Error: {str(e)}"
        print(f"  ❌ Temp file operations: {str(e)}")
    
    # Test directory access
    directories_to_test = ['/opt/airflow', '/opt/airflow/dags', '/opt/airflow/logs']
    
    for directory in directories_to_test:
        try:
            if os.path.exists(directory) and os.access(directory, os.R_OK):
                results[f'access_{directory}'] = 'OK'
                print(f"  ✅ Directory {directory}: accessible")
            else:
                results[f'access_{directory}'] = 'Not accessible'
                print(f"  ⚠️  Directory {directory}: not accessible")
        except Exception as e:
            results[f'access_{directory}'] = f"Error: {str(e)}"
            print(f"  ❌ Directory {directory}: {str(e)}")
    
    return {
        'status': 'success',
        'file_operations': results,
        'previous_results': connectivity_result,
        'timestamp': datetime.now().isoformat()
    }

# Define task dependencies
with dag:
    basic_task = test_basic_python()
    variables_task = test_airflow_variables()
    connectivity_task = test_network_connectivity(basic_task)
    file_ops_task = test_file_operations(connectivity_task)
    
    [basic_task, variables_task] >> connectivity_task >> file_ops_task