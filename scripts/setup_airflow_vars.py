#!/usr/bin/env python3
"""
Setup script to configure Airflow variables for the PySpark streaming DAG.
Run this script to set up all required variables before running the DAG.
"""

import requests
import json
from typing import Dict, Any

# Airflow configuration
AIRFLOW_API_URL = "http://localhost:8080/api/v2"
AIRFLOW_USER = "airflow"
AIRFLOW_PASSWORD = "airflow"

# Variables to set
AIRFLOW_VARIABLES = {
    "REDPANDA_BROKERS": "localhost:19092",
    "WEBHOOK_TOPIC": "webhooks",
    "MINIO_ENDPOINT": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin123",
    "MINIO_BUCKET": "webhook-data",
    "LOCAL_DATA_PATH": "/opt/airflow/data/processed",
    "CHECKPOINT_PATH": "/opt/airflow/data/checkpoints",
    "PROCESSING_WINDOW_MINUTES": "5"
}

def set_airflow_variable(key: str, value: str) -> bool:
    """Set a single Airflow variable via API."""
    url = f"{AIRFLOW_API_URL}/variables"
    
    payload = {
        "key": key,
        "value": value
    }
    
    try:
        # First try to get the variable to see if it exists
        get_response = requests.get(
            f"{url}/{key}",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=5
        )
        
        if get_response.status_code == 200:
            # Variable exists, update it with PATCH
            response = requests.patch(
                f"{url}/{key}",
                json=payload,
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                headers={"Content-Type": "application/json"},
                timeout=5
            )
        else:
            # Variable doesn't exist, create it with POST
            response = requests.post(
                url,
                json=payload,
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                headers={"Content-Type": "application/json"},
                timeout=5
            )
        
        if response.status_code in [200, 201]:
            print(f"  ✅ Set {key} = {value}")
            return True
        else:
            print(f"  ❌ Failed to set {key}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"  ❌ Error setting {key}: {e}")
        return False

def check_airflow_connection() -> bool:
    """Check if Airflow API is accessible."""
    try:
        response = requests.get(
            f"{AIRFLOW_API_URL}/monitor/health",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=5
        )
        return response.status_code == 200
    except:
        return False

def list_current_variables() -> Dict[str, Any]:
    """List all current Airflow variables."""
    try:
        response = requests.get(
            f"{AIRFLOW_API_URL}/variables",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            variables = {var['key']: var['value'] for var in data.get('variables', [])}
            return variables
        else:
            print(f"❌ Failed to get variables: {response.status_code}")
            return {}
    except Exception as e:
        print(f"❌ Error getting variables: {e}")
        return {}

def main():
    """Main setup function."""
    print("🔧 Setting up Airflow variables for PySpark Streaming DAG")
    print("=" * 60)
    
    # Check Airflow connection
    print("🔍 Checking Airflow API connection...")
    if not check_airflow_connection():
        print("❌ Cannot connect to Airflow API. Please ensure:")
        print("  - Airflow is running: docker-compose up -d")
        print("  - API is accessible at: http://localhost:8080")
        print("  - Credentials are correct: airflow/airflow")
        return False
    
    print("✅ Airflow API connection successful")
    
    # Show current variables
    print("\n📋 Current Airflow variables:")
    current_vars = list_current_variables()
    if current_vars:
        for key, value in current_vars.items():
            if key in AIRFLOW_VARIABLES:
                print(f"  {key}: {value}")
    else:
        print("  (No variables found)")
    
    # Set variables
    print(f"\n🔧 Setting {len(AIRFLOW_VARIABLES)} variables...")
    success_count = 0
    
    for key, value in AIRFLOW_VARIABLES.items():
        if set_airflow_variable(key, value):
            success_count += 1
    
    # Summary
    print(f"\n📊 Summary:")
    print(f"  ✅ Successfully set: {success_count}/{len(AIRFLOW_VARIABLES)} variables")
    
    if success_count == len(AIRFLOW_VARIABLES):
        print("\n🎉 All variables configured successfully!")
        print("\nNext steps:")
        print("1. Ensure DAG is unpaused in Airflow UI")
        print("2. Run the test script: python scripts/test_dag.py")
        print(f"3. Monitor in Airflow UI: http://localhost:8080/dags/pyspark_streaming_pipeline")
        return True
    else:
        print(f"\n⚠️  Some variables failed to set. Check the errors above.")
        return False

def verify_setup():
    """Verify the setup by checking all required variables."""
    print("\n🔍 Verifying variable setup...")
    
    current_vars = list_current_variables()
    missing_vars = []
    
    for key in AIRFLOW_VARIABLES.keys():
        if key not in current_vars:
            missing_vars.append(key)
        else:
            print(f"  ✅ {key}: {current_vars[key]}")
    
    if missing_vars:
        print(f"\n❌ Missing variables: {missing_vars}")
        return False
    else:
        print(f"\n✅ All {len(AIRFLOW_VARIABLES)} variables are configured correctly!")
        return True

if __name__ == "__main__":
    success = main()
    
    if success:
        verify_setup()
    
    print(f"\n🌐 Airflow UI: http://localhost:8080")
    print(f"📚 DAG ID: pyspark_streaming_pipeline")