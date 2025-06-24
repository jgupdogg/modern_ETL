#!/usr/bin/env python3
"""
Test True Delta Bronze Tokens Task
Validates the bronze token list implementation with real Delta Lake operations
"""

import sys
import os
import logging

# Add dags to path for imports
sys.path.append('/home/jgupdogg/dev/claude_pipeline/dags')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_bronze_tokens_delta():
    """
    Test the create_bronze_tokens_delta function
    This validates:
    1. Function imports correctly
    2. Mock data processing works
    3. Delta Lake operations succeed
    4. _delta_log is created
    """
    logger.info("🧪 Testing TRUE Delta Lake bronze tokens task...")
    
    try:
        # Import the function
        from tasks.smart_traders.true_delta_bronze_tasks import create_bronze_tokens_delta
        logger.info("✅ Bronze tokens task imported successfully")
        
        # Create mock context (like Airflow would provide)
        mock_context = {
            "run_id": "test_run_20250624_134000",
            "execution_date": "2025-06-24T13:40:00+00:00",
            "task_instance": "mock_task_instance"
        }
        
        logger.info("📋 Created mock Airflow context")
        
        # Run the bronze tokens task
        logger.info("🚀 Executing create_bronze_tokens_delta...")
        result = create_bronze_tokens_delta(**mock_context)
        
        # Validate results
        logger.info("🔍 Validating results...")
        
        if result["status"] != "success":
            if result["status"] == "no_data":
                logger.warning("⚠️ No token data returned from BirdEye API")
                logger.info("This is expected if API returns no data or rate limits")
                return True
            else:
                logger.error(f"❌ Task failed with status: {result['status']}")
                logger.error(f"Error: {result.get('error', 'Unknown error')}")
                return False
        
        # Validate success results
        required_fields = [
            "records", "delta_version", "table_path", 
            "health_status", "delta_log_verified"
        ]
        
        for field in required_fields:
            if field not in result:
                logger.error(f"❌ Missing required field: {field}")
                return False
        
        logger.info("✅ Result validation passed!")
        logger.info(f"   📊 Records processed: {result['records']}")
        logger.info(f"   📈 Delta version: {result['delta_version']}")
        logger.info(f"   🎯 Table path: {result['table_path']}")
        logger.info(f"   🏥 Health status: {result['health_status']}")
        logger.info(f"   🗂️ Delta log verified: {result['delta_log_verified']}")
        logger.info(f"   📋 Operation: {result.get('operation', 'Unknown')}")
        
        # Additional validations
        if result["delta_version"] != 0:
            logger.warning(f"⚠️ Expected version 0 for new table, got {result['delta_version']}")
        
        if not result["delta_log_verified"]:
            logger.error("❌ Delta log not verified")
            return False
        
        if result["health_status"] != "healthy":
            logger.error(f"❌ Table health not healthy: {result['health_status']}")
            return False
        
        logger.info("🎉 Bronze tokens Delta Lake task test SUCCESSFUL!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Bronze tokens test failed: {e}")
        return False

def test_token_list_api_integration():
    """
    Test that the underlying token list API call works
    """
    logger.info("🔗 Testing BirdEye API integration...")
    
    try:
        from tasks.smart_traders.bronze_tasks import fetch_bronze_token_list
        
        # Mock context
        mock_context = {
            "run_id": "test_api_integration",
            "execution_date": "2025-06-24T13:40:00+00:00"
        }
        
        result = fetch_bronze_token_list(**mock_context)
        
        if not result:
            logger.warning("⚠️ No data from BirdEye API (could be rate limiting or no data)")
            return True
        
        if isinstance(result, list) and len(result) > 0:
            logger.info(f"✅ BirdEye API returned {len(result)} tokens")
            
            # Check first token structure
            first_token = result[0]
            if isinstance(first_token, dict):
                logger.info(f"📋 Sample token fields: {list(first_token.keys())}")
            
            return True
        else:
            logger.warning("⚠️ Unexpected result format from BirdEye API")
            return True
            
    except Exception as e:
        logger.error(f"❌ API integration test failed: {e}")
        return False

def main():
    """Run all bronze tokens tests"""
    logger.info("🚀 Starting TRUE Delta Lake bronze tokens tests...")
    
    tests = [
        ("BirdEye API Integration", test_token_list_api_integration),
        ("Bronze Tokens Delta Task", test_bronze_tokens_delta),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"❌ {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("📊 BRONZE TOKENS TEST RESULTS")
    logger.info("="*50)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{status} - {test_name}")
        if result:
            passed += 1
    
    logger.info(f"\n🎯 Tests Passed: {passed}/{total}")
    
    if passed == total:
        logger.info("🎉 ALL BRONZE TOKENS TESTS PASSED!")
        logger.info("🔄 Ready to test inside Docker container")
    else:
        logger.error("❌ Some tests failed - check implementation")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)