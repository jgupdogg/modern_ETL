"""
Simple Airflow DAG to update Helius webhook with addresses from gold.top_traders.
This version executes entirely within the Airflow worker container.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import logging
import subprocess

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    'webhook_gold_update_v2',
    default_args=default_args,
    description='Update Helius webhook with gold layer addresses',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test']
)

def get_addresses_from_gold_layer():
    """Get addresses from gold layer using subprocess to call DuckDB container."""
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

result = conn.execute("SELECT DISTINCT wallet_address FROM read_parquet('s3://solana-data/gold/top_traders/*.parquet') WHERE wallet_address IS NOT NULL AND wallet_address != '' ORDER BY wallet_address").fetchall()
addresses = [row[0] for row in result if row[0]]

print("ADDRESSES_START")
for addr in addresses:
    print(addr)
print("ADDRESSES_END")
        '''
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Failed to fetch addresses: {result.stderr}")
    
    # Parse addresses
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
    
    return addresses

def update_webhook_task(**context):
    """Main task to update webhook."""
    logger = logging.getLogger(__name__)
    
    try:
        # Get addresses from gold layer
        logger.info("🥇 Fetching addresses from gold layer...")
        addresses = get_addresses_from_gold_layer()
        logger.info(f"✅ Found {len(addresses)} addresses")
        
        # Store count in Airflow Variable
        Variable.set("webhook_gold_addresses_count", str(len(addresses)))
        Variable.set("webhook_last_update", datetime.now().isoformat())
        
        # Log addresses (first 5 as sample)
        logger.info("Sample addresses:")
        for i, addr in enumerate(addresses[:5]):
            logger.info(f"  {i+1}. {addr}")
        
        if len(addresses) > 5:
            logger.info(f"  ... and {len(addresses) - 5} more")
        
        # Get HELIUS_API_KEY from Airflow Variable
        api_key = Variable.get("HELIUS_API_KEY", default_var=None)
        if not api_key:
            raise Exception("HELIUS_API_KEY not set in Airflow Variables")
        
        # Update webhook using httpx
        logger.info("🌐 Updating Helius webhook...")
        
        # Install httpx if needed
        subprocess.run(['pip', 'install', 'httpx'], capture_output=True)
        
        # Create update script
        update_cmd = f'''
import httpx
import asyncio
import json

async def update():
    api_key = "{api_key}"
    addresses = {addresses}
    
    base_url = "https://api.helius.xyz/v0"
    headers = {{"Content-Type": "application/json"}}
    
    async with httpx.AsyncClient() as client:
        # Get existing webhook
        response = await client.get(f"{{base_url}}/webhooks?api-key={{api_key}}", headers=headers)
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
        
        response = await client.put(
            f"{{base_url}}/webhooks/{{webhook_id}}?api-key={{api_key}}",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        
        print(f"SUCCESS: Updated webhook {{webhook_id}} with {{len(addresses)}} addresses")

asyncio.run(update())
        '''
        
        result = subprocess.run(['python3', '-c', update_cmd], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✅ Webhook update successful!")
            logger.info(f"Output: {result.stdout}")
            return {"status": "success", "addresses_count": len(addresses)}
        else:
            raise Exception(f"Update failed: {result.stderr}")
            
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

# Create task
update_webhook = PythonOperator(
    task_id='update_webhook_from_gold',
    python_callable=update_webhook_task,
    dag=dag,
)