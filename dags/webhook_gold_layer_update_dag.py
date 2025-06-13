"""
Simple Airflow DAG to update Helius webhook with addresses from gold.top_traders.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import logging
import subprocess
import os

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
    'webhook_gold_layer_update',
    default_args=default_args,
    description='Update Helius webhook with addresses from gold.top_traders',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test']
)

def update_webhook_from_gold_layer(**context):
    """
    Update Helius webhook with addresses from gold layer.
    Runs the update logic directly using DuckDB container.
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Step 1: Get addresses from gold layer using DuckDB container
        logger.info("🥇 Fetching addresses from gold layer...")
        
        cmd = [
            'docker', 'exec', 'claude_pipeline-duckdb', 'python3', '-c',
            '''
import duckdb
conn = duckdb.connect("/data/analytics.duckdb")
conn.execute("LOAD httpfs;")
conn.execute("SET s3_endpoint='minio:9000';")
conn.execute("SET s3_access_key_id='minioadmin';")
conn.execute("SET s3_secret_access_key='minioadmin123';")
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_url_style='path';")

query = "SELECT DISTINCT wallet_address FROM read_parquet('s3://solana-data/gold/top_traders/*.parquet') WHERE wallet_address IS NOT NULL AND wallet_address != '' ORDER BY wallet_address"

result = conn.execute(query).fetchall()
addresses = [row[0] for row in result if row[0]]

print("ADDRESSES_START")
for addr in addresses:
    print(addr)
print("ADDRESSES_END")
conn.close()
            '''
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Failed to fetch addresses: {result.stderr}")
        
        # Parse addresses from output
        lines = result.stdout.split('\n')
        in_addresses = False
        addresses = []
        
        for line in lines:
            if line == 'ADDRESSES_START':
                in_addresses = True
                continue
            elif line == 'ADDRESSES_END':
                break
            elif in_addresses and line.strip():
                addresses.append(line.strip())
        
        logger.info(f"✅ Found {len(addresses)} addresses in gold layer")
        
        # Store in Airflow Variable for reference
        Variable.set("webhook_gold_addresses_count", len(addresses))
        
        # Step 2: Update Helius webhook using Python script in worker
        logger.info("🌐 Updating Helius webhook...")
        
        # Create a temporary Python script with the update logic
        update_script = f'''
import os
os.environ["HELIUS_API_KEY"] = "{os.environ.get('HELIUS_API_KEY', '')}"
addresses = {addresses}

import httpx
import asyncio

async def update_webhook():
    api_key = os.environ["HELIUS_API_KEY"]
    if not api_key:
        raise Exception("HELIUS_API_KEY not set")
    
    base_url = "https://api.helius.xyz/v0"
    headers = {{"Content-Type": "application/json"}}
    
    # Get existing webhook
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{{base_url}}/webhooks?api-key={{api_key}}",
            headers=headers
        )
        response.raise_for_status()
        webhooks = response.json()
    
    if not webhooks:
        raise Exception("No webhooks found")
    
    webhook = webhooks[0]
    webhook_id = webhook.get("webhookID")
    
    # Update webhook
    payload = {{
        "webhookURL": webhook.get("webhookURL"),
        "transactionTypes": webhook.get("transactionTypes", ["SWAP"]),
        "accountAddresses": addresses,
        "webhookType": webhook.get("webhookType", "enhanced")
    }}
    
    async with httpx.AsyncClient() as client:
        response = await client.put(
            f"{{base_url}}/webhooks/{{webhook_id}}?api-key={{api_key}}",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
    
    print(f"SUCCESS: Updated webhook with {{len(addresses)}} addresses")

asyncio.run(update_webhook())
        '''
        
        # Execute the update script
        update_result = subprocess.run(
            ['python3', '-c', update_script],
            capture_output=True,
            text=True
        )
        
        if update_result.returncode == 0:
            logger.info(f"✅ Webhook update successful!")
            logger.info(f"Output: {update_result.stdout}")
            Variable.set("webhook_last_update", datetime.now().isoformat())
            return True
        else:
            logger.error(f"❌ Webhook update failed!")
            logger.error(f"Error: {update_result.stderr}")
            raise Exception(f"Update failed: {update_result.stderr}")
            
    except Exception as e:
        logger.error(f"Error updating webhook: {str(e)}")
        raise

# Create task
update_webhook_task = PythonOperator(
    task_id='update_webhook_from_gold_layer',
    python_callable=update_webhook_from_gold_layer,
    dag=dag,
)