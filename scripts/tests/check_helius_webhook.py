#!/usr/bin/env python3
"""
Check current Helius webhook configuration
"""

import os
import asyncio
import sys
import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/home/jgupdogg/dev/claude_pipeline/.env')

class HeliusChecker:
    def __init__(self):
        self.api_key = os.getenv("HELIUS_API_KEY")
        if not self.api_key:
            raise ValueError("HELIUS_API_KEY is not set")
        
        self.base_url = "https://api.helius.xyz/v0"
        self.headers = {"Content-Type": "application/json"}
    
    async def get_all_webhooks(self):
        """Get all existing webhooks."""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/webhooks?api-key={self.api_key}",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                print(f"Error fetching webhooks: {e}")
                return []

async def main():
    print("🔍 CHECKING HELIUS WEBHOOK CONFIGURATION")
    print("=" * 50)
    
    try:
        checker = HeliusChecker()
        webhooks = await checker.get_all_webhooks()
        
        if not webhooks:
            print("❌ No webhooks found")
            return
        
        for i, webhook in enumerate(webhooks):
            print(f"\n📡 Webhook {i+1}:")
            print(f"  ID: {webhook.get('webhookID', 'N/A')}")
            print(f"  URL: {webhook.get('webhookURL', 'N/A')}")
            print(f"  Type: {webhook.get('webhookType', 'N/A')}")
            print(f"  Transaction Types: {webhook.get('transactionTypes', [])}")
            
            addresses = webhook.get('accountAddresses', [])
            print(f"  Monitored Addresses ({len(addresses)}):")
            
            if addresses:
                for j, addr in enumerate(addresses[:10]):  # Show first 10
                    print(f"    {j+1:2d}. {addr}")
                if len(addresses) > 10:
                    print(f"    ... and {len(addresses) - 10} more addresses")
            else:
                print("    No addresses configured")
        
        print(f"\n✅ Found {len(webhooks)} webhook(s)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())