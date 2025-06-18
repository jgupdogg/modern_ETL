#!/usr/bin/env python3
"""
Simple test for the new raw bronze pipeline implementation
Tests without external dependencies
"""

import sys
import os
import json
from datetime import datetime

# Add dags directory to path for imports
sys.path.append('/home/jgupdogg/dev/claude_pipeline/dags')

def test_schema_import():
    """Test importing the new raw transaction schema"""
    print("🧪 Testing Schema Import")
    print("=" * 30)
    
    try:
        from tasks.bronze_tasks import get_raw_transaction_schema
        schema = get_raw_transaction_schema()
        
        print(f"✅ Schema imported successfully")
        print(f"📊 Schema has {len(schema)} fields")
        
        # Check for key fields
        field_names = [field.name for field in schema]
        expected_fields = [
            'wallet_address', 'transaction_hash', 'timestamp',
            'base_type_swap', 'quote_type_swap', 
            'processed_for_pnl', 'data_source'
        ]
        
        missing = [f for f in expected_fields if f not in field_names]
        if missing:
            print(f"❌ Missing fields: {missing}")
            return False
        
        print("✅ All key fields present")
        return True
        
    except Exception as e:
        print(f"❌ Schema import failed: {e}")
        return False


def test_transformation_import():
    """Test importing the raw transformation function"""
    print("\n🧪 Testing Transformation Import")
    print("=" * 35)
    
    try:
        from tasks.bronze_tasks import transform_trade_raw
        print("✅ Raw transformation function imported successfully")
        return True
        
    except Exception as e:
        print(f"❌ Transformation import failed: {e}")
        return False


def test_transformation_logic():
    """Test the transformation logic with mock data"""
    print("\n🧪 Testing Transformation Logic")
    print("=" * 35)
    
    try:
        from tasks.bronze_tasks import transform_trade_raw
        
        # Mock timestamp class to avoid pandas dependency
        class MockTimestamp:
            def __init__(self, value):
                self.value = value
            def isoformat(self):
                return self.value
        
        # Simple test trade
        test_trade = {
            "tx_hash": "test_hash_123",
            "owner": "test_wallet",
            "base": {
                "symbol": "SOL",
                "address": "So11111111111111111111111111111111111111112",
                "type_swap": "to",
                "ui_change_amount": 1.5,
                "nearest_price": 200.0,
                "decimals": 9
            },
            "quote": {
                "symbol": "USDC", 
                "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "type_swap": "from",
                "ui_change_amount": -300.0,
                "nearest_price": 1.0,
                "decimals": 6
            },
            "source": "raydium",
            "tx_type": "swap",
            "block_unix_time": 1731555934
        }
        
        wallet_address = "test_wallet"
        fetched_at = MockTimestamp("2025-06-18T10:00:00+00:00")
        
        result = transform_trade_raw(test_trade, wallet_address, fetched_at)
        
        if result is None:
            print("❌ Transformation returned None")
            return False
        
        print("✅ Transformation successful")
        
        # Check key fields
        checks = [
            ('transaction_hash', 'test_hash_123'),
            ('wallet_address', 'test_wallet'),
            ('base_symbol', 'SOL'),
            ('base_type_swap', 'to'),
            ('quote_symbol', 'USDC'),
            ('quote_type_swap', 'from'),
            ('processed_for_pnl', False),
            ('data_source', 'birdeye_v3')
        ]
        
        all_good = True
        for field, expected in checks:
            actual = result.get(field)
            if actual == expected:
                print(f"  ✅ {field}: {actual}")
            else:
                print(f"  ❌ {field}: expected {expected}, got {actual}")
                all_good = False
        
        return all_good
        
    except Exception as e:
        print(f"❌ Transformation logic test failed: {e}")
        import traceback
        print(traceback.format_exc())
        return False


def test_silver_imports():
    """Test importing the new silver processing functions"""
    print("\n🧪 Testing Silver Layer Imports")
    print("=" * 35)
    
    try:
        from tasks.silver_tasks import process_raw_bronze_pnl
        print("✅ Raw bronze PnL processing function imported")
        
        from tasks.silver_tasks import normalize_raw_transactions
        print("✅ Normalization function imported")
        
        from tasks.silver_tasks import get_comprehensive_pnl_schema
        print("✅ Comprehensive PnL schema function imported")
        
        return True
        
    except Exception as e:
        print(f"❌ Silver imports failed: {e}")
        return False


def test_swap_direction_concepts():
    """Test swap direction determination concepts"""
    print("\n🧪 Testing Swap Direction Concepts")
    print("=" * 40)
    
    try:
        # Test case: USDC → SOL swap
        print("📊 Test Case: USDC → SOL")
        
        trade = {
            "base": {"type_swap": "to", "symbol": "SOL", "ui_change_amount": 0.5},
            "quote": {"type_swap": "from", "symbol": "USDC", "ui_change_amount": -100.0}
        }
        
        # Determine sold/bought tokens
        if trade["base"]["type_swap"] == "from":
            sold_token = trade["base"]
            bought_token = trade["quote"]
        else:
            sold_token = trade["quote"]
            bought_token = trade["base"]
        
        print(f"  🔴 SOLD: {abs(sold_token['ui_change_amount'])} {sold_token['symbol']}")
        print(f"  🟢 BOUGHT: {abs(bought_token['ui_change_amount'])} {bought_token['symbol']}")
        
        # Verify logic
        if sold_token["symbol"] == "USDC" and bought_token["symbol"] == "SOL":
            print("  ✅ Direction logic correct")
            return True
        else:
            print("  ❌ Direction logic incorrect")
            return False
            
    except Exception as e:
        print(f"❌ Swap direction test failed: {e}")
        return False


def main():
    """Run simplified tests"""
    print("🚀 Simple Raw Bronze Pipeline Test")
    print("=" * 50)
    
    tests = [
        ("Schema Import", test_schema_import),
        ("Transformation Import", test_transformation_import),
        ("Transformation Logic", test_transformation_logic),
        ("Silver Imports", test_silver_imports),
        ("Swap Direction Concepts", test_swap_direction_concepts)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{'='*50}")
    print("📋 TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 Results: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 All tests passed! Core implementation is working.")
    else:
        print("⚠️  Some tests failed. Check implementation.")
    
    return passed == total


if __name__ == "__main__":
    main()