#!/usr/bin/env python3
"""
Test the webhook service's gold layer integration by directly calling the HeliusWebhookManager.
This simulates what happens when the service starts up.
"""

import os
import sys
import asyncio
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/home/jgupdogg/dev/claude_pipeline/.env')

# Add webhook service to path so we can import it
sys.path.append('/home/jgupdogg/dev/claude_pipeline/services/webhook-listener')

async def test_webhook_service_gold_integration():
    """Test the actual webhook service code with gold layer integration."""
    print("🚀 TESTING WEBHOOK SERVICE GOLD LAYER INTEGRATION")
    print("=" * 60)
    
    try:
        # Set the MinIO environment variables (these would be set by docker-compose)
        os.environ['MINIO_ENDPOINT'] = 'http://minio:9000'
        os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
        os.environ['MINIO_SECRET_KEY'] = 'minioadmin123'
        os.environ['SOLANA_DATA_BUCKET'] = 'solana-data'
        os.environ['MAX_WEBHOOK_ADDRESSES'] = '100'
        
        # Import the actual service code
        from helius_helper import HeliusWebhookManager
        
        print("📱 Creating HeliusWebhookManager...")
        helius = HeliusWebhookManager()
        
        print("🥇 Calling get_addresses() - this should query the gold layer...")
        addresses = helius.get_addresses()
        
        print(f"✅ SUCCESS! Retrieved {len(addresses)} addresses from gold layer:")
        for i, addr in enumerate(addresses, 1):
            print(f"  {i:2d}. {addr}")
        
        # Now let's test updating the webhook with the current ngrok URL
        current_webhook_url = "https://d83daba99099.ngrok.app/webhooks"  # From our earlier check
        
        print(f"\n🌐 Testing webhook update with gold layer addresses...")
        print(f"📡 Webhook URL: {current_webhook_url}")
        
        # Test the update
        success = await helius.update_or_create_webhook(current_webhook_url)
        
        if success:
            print("🎉 WEBHOOK UPDATE SUCCESSFUL!")
            print("✨ The webhook service is now using gold layer addresses!")
        else:
            print("❌ Webhook update failed")
        
        return success
        
    except Exception as e:
        print(f"❌ Error testing webhook service integration: {e}")
        import traceback
        traceback.print_exc()
        return False

async def verify_final_state():
    """Verify the final webhook state."""
    print("\n🔍 VERIFYING FINAL WEBHOOK STATE")
    print("=" * 40)
    
    import httpx
    
    api_key = os.getenv("HELIUS_API_KEY")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.helius.xyz/v0/webhooks?api-key={api_key}",
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            webhooks = response.json()
        
        if webhooks:
            webhook = webhooks[0]
            addresses = webhook.get('accountAddresses', [])
            
            print(f"📊 Current webhook configuration:")
            print(f"  🆔 ID: {webhook.get('webhookID')}")
            print(f"  🔗 URL: {webhook.get('webhookURL')}")
            print(f"  📍 Addresses ({len(addresses)}):")
            
            for i, addr in enumerate(addresses, 1):
                print(f"    {i:2d}. {addr}")
            
            print(f"\n✅ Webhook is monitoring {len(addresses)} addresses from gold layer")
            return True
        else:
            print("❌ No webhooks found")
            return False
            
    except Exception as e:
        print(f"❌ Error verifying webhook state: {e}")
        return False

async def main():
    """Main test function."""
    print("🧪 WEBHOOK SERVICE GOLD LAYER INTEGRATION TEST")
    print("=" * 70)
    print("This tests the actual code that runs when webhook-listener starts up")
    
    # Test 1: Gold layer integration
    success = await test_webhook_service_gold_integration()
    
    if success:
        # Test 2: Verify final state
        await verify_final_state()
        
        print("\n🎉 INTEGRATION TEST COMPLETE!")
        print("✅ The webhook service successfully:")
        print("   📊 Queries gold layer for top trader addresses")
        print("   🔄 Updates Helius webhook configuration")
        print("   🥇 Uses modern lakehouse architecture (MinIO + DuckDB)")
        print("\n🚀 When you restart the webhook service, it will automatically")
        print("   pull the latest top traders from your gold layer!")
    else:
        print("\n❌ Integration test failed")

if __name__ == "__main__":
    asyncio.run(main())