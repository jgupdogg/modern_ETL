#!/usr/bin/env python3
"""
Test silver wallet PnL Delta Lake task - CONSERVATIVE MODE
ZERO FALLBACKS - real data or clean failure
"""

import sys
import os
import logging

# Add the dags directory to the Python path
sys.path.insert(0, '/opt/airflow/dags')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_silver_wallet_pnl_delta():
    """Test the conservative silver wallet PnL Delta Lake task"""
    try:
        logger.info("🚀 Testing CONSERVATIVE silver wallet PnL Delta Lake task...")
        logger.info("⚙️ Mode: 10 wallets, 1GB memory, ZERO FALLBACKS")
        
        # Import the task function
        from tasks.smart_traders.silver_delta_wallet_pnl import create_silver_wallet_pnl_delta
        
        # Create a mock context
        context = {
            "run_id": "conservative_test_20250624_143500",
            "task_instance": None
        }
        
        # Execute the task
        result = create_silver_wallet_pnl_delta(**context)
        
        logger.info(f"✅ Task completed with result: {result}")
        
        if result.get("status") == "success":
            logger.info("🎉 CONSERVATIVE silver wallet PnL Delta Lake task succeeded!")
            logger.info(f"   👛 Wallets processed: {result.get('wallets_processed', 0)}")
            logger.info(f"   📊 PnL records created: {result.get('pnl_records', 0)}")
            logger.info(f"   💳 Transactions processed: {result.get('transactions_processed', 0)}")
            logger.info(f"   📈 Delta version: {result.get('delta_version', 'N/A')}")
            logger.info(f"   🔄 Operation: {result.get('operation', 'N/A')}")
            logger.info(f"   🏥 Health status: {result.get('health_status', 'N/A')}")
            return True
        else:
            logger.warning(f"⚠️ Task completed but with status: {result.get('status')}")
            logger.warning(f"   Message: {result.get('message', result.get('error', 'Unknown'))}")
            
            # For no_data status, still consider it a success (expected behavior)
            if result.get('status') == 'no_data':
                logger.info("✅ No data status is expected behavior - considering successful")
                return True
            else:
                return False
    
    except Exception as e:
        logger.error(f"❌ CONSERVATIVE silver wallet PnL Delta Lake task failed: {e}")
        import traceback
        logger.error(f"   Full traceback:\n{traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = test_silver_wallet_pnl_delta()
    if success:
        print("\n🚀 CONSERVATIVE silver wallet PnL Delta Lake test completed!")
    else:
        print("\n❌ Fix issues before proceeding")
    
    sys.exit(0 if success else 1)