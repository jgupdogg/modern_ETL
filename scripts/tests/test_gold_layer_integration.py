#!/usr/bin/env python3
"""
Test script to verify gold layer integration for webhook addresses.
"""

import os
import sys
import asyncio
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/home/jgupdogg/dev/claude_pipeline/.env')

# Add webhook service to path
sys.path.append('/home/jgupdogg/dev/claude_pipeline/services/webhook-listener')

def test_gold_layer_query():
    """Test direct DuckDB query to gold layer."""
    import duckdb
    
    print("🔍 TESTING GOLD LAYER QUERY")
    print("=" * 50)
    
    conn = duckdb.connect()
    
    try:
        # Configure S3/MinIO access
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_endpoint='minio:9000';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin123';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        # Test query - get ALL unique addresses
        query = """
        SELECT DISTINCT wallet_address 
        FROM read_parquet('s3://solana-data/gold/top_traders/*.parquet') 
        WHERE wallet_address IS NOT NULL AND wallet_address != ''
        ORDER BY wallet_address
        """
        
        result = conn.execute(query).fetchall()
        
        print(f"✅ Successfully queried gold layer")
        print(f"📊 Found {len(result)} unique wallet addresses:")
        
        for i, row in enumerate(result[:10], 1):  # Show first 10
            wallet = row[0]
            print(f"  {i:2d}. {wallet[:8]}...{wallet[-8:]}")
        
        if len(result) > 10:
            print(f"  ... and {len(result) - 10} more addresses")
        
        # Test address extraction
        addresses = [row[0] for row in result if row[0]]
        print(f"\n📋 Extracted {len(addresses)} addresses for webhook")
        
        return addresses
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []
    finally:
        conn.close()

async def test_helius_integration():
    """Test HeliusWebhookManager with gold layer integration."""
    print("\n🌐 TESTING HELIUS INTEGRATION")
    print("=" * 50)
    
    try:
        # Set environment variables for testing
        os.environ['MINIO_ENDPOINT'] = 'http://minio:9000'
        os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
        os.environ['MINIO_SECRET_KEY'] = 'minioadmin123'
        os.environ['SOLANA_DATA_BUCKET'] = 'solana-data'
        os.environ['MAX_WEBHOOK_ADDRESSES'] = '10'
        
        from helius_helper import HeliusWebhookManager
        
        helius = HeliusWebhookManager()
        addresses = helius.get_addresses()
        
        print(f"✅ HeliusWebhookManager integration working")
        print(f"📍 Retrieved {len(addresses)} addresses:")
        
        for i, addr in enumerate(addresses[:10], 1):
            print(f"  {i:2d}. {addr}")
        
        if len(addresses) > 10:
            print(f"  ... and {len(addresses) - 10} more")
        
        return addresses
        
    except Exception as e:
        print(f"❌ Integration error: {e}")
        import traceback
        traceback.print_exc()
        return []

async def compare_with_current_webhook():
    """Compare gold layer addresses with current webhook configuration."""
    print("\n🔄 COMPARING WITH CURRENT WEBHOOK")
    print("=" * 50)
    
    try:
        # Get current webhook addresses
        import httpx
        api_key = os.getenv("HELIUS_API_KEY")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.helius.xyz/v0/webhooks?api-key={api_key}",
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            webhooks = response.json()
        
        current_addresses = set()
        if webhooks:
            current_addresses = set(webhooks[0].get('accountAddresses', []))
        
        # Get gold layer addresses
        os.environ['MINIO_ENDPOINT'] = 'http://minio:9000'
        os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
        os.environ['MINIO_SECRET_KEY'] = 'minioadmin123'
        
        from helius_helper import HeliusWebhookManager
        helius = HeliusWebhookManager()
        gold_addresses = set(helius.get_addresses())
        
        print(f"📍 Current webhook addresses: {len(current_addresses)}")
        print(f"🥇 Gold layer addresses: {len(gold_addresses)}")
        
        # Show differences
        new_addresses = gold_addresses - current_addresses
        removed_addresses = current_addresses - gold_addresses
        
        if new_addresses:
            print(f"\n🆕 NEW addresses from gold layer ({len(new_addresses)}):")
            for addr in list(new_addresses)[:5]:
                print(f"  + {addr}")
        
        if removed_addresses:
            print(f"\n🗑️  REMOVED from current webhook ({len(removed_addresses)}):")
            for addr in list(removed_addresses)[:5]:
                print(f"  - {addr}")
        
        if not new_addresses and not removed_addresses:
            print("✅ No changes needed - addresses match!")
        
    except Exception as e:
        print(f"❌ Comparison error: {e}")

async def main():
    """Run all tests."""
    print("🚀 GOLD LAYER WEBHOOK INTEGRATION TEST")
    print("=" * 60)
    
    # Test 1: Direct DuckDB query
    gold_addresses = test_gold_layer_query()
    
    if not gold_addresses:
        print("❌ No addresses found in gold layer - stopping tests")
        return
    
    # Test 2: HeliusWebhookManager integration
    await test_helius_integration()
    
    # Test 3: Compare with current webhook
    await compare_with_current_webhook()
    
    print("\n✅ Integration testing complete!")
    print("🎯 Ready to implement dynamic address updates")

if __name__ == "__main__":
    asyncio.run(main())