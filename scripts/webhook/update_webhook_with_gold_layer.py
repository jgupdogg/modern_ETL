#!/usr/bin/env python3
"""
Update Helius webhook with addresses from gold layer.
This simulates what the webhook service will do.
"""

import os
import asyncio
import subprocess
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/home/jgupdogg/dev/claude_pipeline/.env')

async def get_gold_layer_addresses():
    """Get addresses from gold layer using DuckDB container."""
    cmd = [
        'docker', 'exec', 'claude_pipeline-duckdb', 'python3', '-c',
        '''
import duckdb
conn = duckdb.connect("/data/analytics.duckdb")
conn.execute("LOAD httpfs;")
conn.execute("SET s3_endpoint=\\"minio:9000\\";")
conn.execute("SET s3_access_key_id=\\"minioadmin\\";")
conn.execute("SET s3_secret_access_key=\\"minioadmin123\\";")
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_url_style=\\"path\\";")

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
    
    if result.returncode == 0:
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
    else:
        print(f"Error getting addresses: {result.stderr}")
        return []

async def update_helius_webhook(addresses):
    """Update Helius webhook with new addresses."""
    import httpx
    
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        print("❌ HELIUS_API_KEY not found")
        return False
    
    base_url = "https://api.helius.xyz/v0"
    headers = {"Content-Type": "application/json"}
    
    try:
        # Get existing webhooks
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{base_url}/webhooks?api-key={api_key}",
                headers=headers
            )
            response.raise_for_status()
            webhooks = response.json()
        
        if not webhooks:
            print("❌ No existing webhooks found")
            return False
        
        webhook = webhooks[0]
        webhook_id = webhook.get("webhookID")
        current_addresses = set(webhook.get('accountAddresses', []))
        new_addresses = set(addresses)
        
        print(f"📍 Current addresses: {len(current_addresses)}")
        print(f"🥇 Gold layer addresses: {len(new_addresses)}")
        
        # Show differences
        added = new_addresses - current_addresses
        removed = current_addresses - new_addresses
        
        if added:
            print(f"🆕 Adding {len(added)} addresses:")
            for addr in list(added)[:5]:
                print(f"  + {addr}")
        
        if removed:
            print(f"🗑️  Removing {len(removed)} addresses:")
            for addr in list(removed)[:5]:
                print(f"  - {addr}")
        
        if not added and not removed:
            print("✅ No changes needed - addresses already match!")
            return True
        
        # Update webhook
        payload = {
            "webhookURL": webhook.get("webhookURL"),
            "transactionTypes": webhook.get("transactionTypes", ["SWAP"]),
            "accountAddresses": addresses,
            "webhookType": webhook.get("webhookType", "enhanced")
        }
        
        auth_header = os.getenv("HELIUS_AUTH_HEADER")
        if auth_header:
            payload["authHeader"] = auth_header
        
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}/webhooks/{webhook_id}?api-key={api_key}",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            result = response.json()
        
        print(f"✅ Webhook updated successfully!")
        print(f"🔗 Webhook ID: {webhook_id}")
        print(f"📊 Now monitoring {len(addresses)} addresses")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating webhook: {e}")
        return False

async def main():
    """Main function to update webhook with gold layer addresses."""
    print("🚀 UPDATING HELIUS WEBHOOK WITH GOLD LAYER")
    print("=" * 50)
    
    # Get addresses from gold layer
    print("🥇 Fetching addresses from gold layer...")
    addresses = await get_gold_layer_addresses()
    
    if not addresses:
        print("❌ No addresses found in gold layer")
        return
    
    print(f"✅ Found {len(addresses)} addresses in gold layer:")
    for i, addr in enumerate(addresses, 1):
        print(f"  {i}. {addr}")
    
    # Update webhook
    print(f"\\n🌐 Updating Helius webhook...")
    success = await update_helius_webhook(addresses)
    
    if success:
        print("\\n🎉 Webhook successfully updated with gold layer addresses!")
        print("📡 Helius will now monitor top traders from your gold layer")
    else:
        print("\\n❌ Failed to update webhook")

if __name__ == "__main__":
    asyncio.run(main())