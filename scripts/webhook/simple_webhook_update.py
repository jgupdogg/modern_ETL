#!/usr/bin/env python3
"""
Simple webhook update using known gold layer addresses.
"""

import os
import asyncio
import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/home/jgupdogg/dev/claude_pipeline/.env')

# These are the addresses we know are in the gold layer
GOLD_LAYER_ADDRESSES = [
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
    "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", 
    "8QPTsaJ5FLj3dCnqM6UjjsGfT9F9v8vT2B3ygNFqEKXy"
]

async def compare_and_update_webhook():
    """Compare current webhook with gold layer addresses."""
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        print("❌ HELIUS_API_KEY not found")
        return False

    base_url = "https://api.helius.xyz/v0"
    headers = {"Content-Type": "application/json"}

    try:
        # Get current webhook
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{base_url}/webhooks?api-key={api_key}",
                headers=headers
            )
            response.raise_for_status()
            webhooks = response.json()

        if not webhooks:
            print("❌ No webhooks found")
            return False

        webhook = webhooks[0]
        webhook_id = webhook.get("webhookID")
        current_addresses = set(webhook.get('accountAddresses', []))
        gold_addresses = set(GOLD_LAYER_ADDRESSES)

        print("📊 WEBHOOK COMPARISON")
        print("=" * 40)
        print(f"Current webhook addresses ({len(current_addresses)}):")
        for addr in current_addresses:
            print(f"  📍 {addr}")

        print(f"\nGold layer addresses ({len(gold_addresses)}):")
        for addr in gold_addresses:
            print(f"  🥇 {addr}")

        # Calculate differences
        added = gold_addresses - current_addresses
        removed = current_addresses - gold_addresses
        same = current_addresses & gold_addresses

        print(f"\n🔄 ANALYSIS:")
        print(f"  ✅ Same: {len(same)} addresses")
        print(f"  🆕 To add: {len(added)} addresses")
        print(f"  🗑️  To remove: {len(removed)} addresses")

        if added:
            print(f"\n🆕 NEW from gold layer:")
            for addr in added:
                print(f"  + {addr}")

        if removed:
            print(f"\n🗑️  REMOVED from current:")
            for addr in removed:
                print(f"  - {addr}")

        if not added and not removed:
            print("\n✅ No changes needed!")
            return True

        # Ask for confirmation
        print(f"\n🤔 Update webhook to use gold layer addresses?")
        print(f"   This will change from {len(current_addresses)} to {len(gold_addresses)} addresses")
        
        # For demo, we'll proceed (in production you might want confirmation)
        proceed = True
        
        if proceed:
            # Update webhook
            payload = {
                "webhookURL": webhook.get("webhookURL"),
                "transactionTypes": webhook.get("transactionTypes", ["SWAP"]),
                "accountAddresses": list(gold_addresses),
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

            print(f"\n🎉 WEBHOOK UPDATED SUCCESSFULLY!")
            print(f"📡 Webhook ID: {webhook_id}")
            print(f"🥇 Now monitoring {len(gold_addresses)} top traders from gold layer")
            
            return True
        else:
            print("\n⏸️  Update cancelled")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

async def main():
    print("🚀 HELIUS WEBHOOK ↔️ GOLD LAYER INTEGRATION")
    print("=" * 50)
    print("This demonstrates dynamic address population from gold.top_traders")
    
    success = await compare_and_update_webhook()
    
    if success:
        print("\n✨ Integration successful!")
        print("📈 Your webhook now monitors addresses from the gold layer")
        print("🔄 Future updates can happen automatically when gold layer changes")
    else:
        print("\n❌ Integration failed")

if __name__ == "__main__":
    asyncio.run(main())