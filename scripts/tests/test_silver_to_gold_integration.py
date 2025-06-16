#!/usr/bin/env python3
"""
Test script to verify silver to gold layer integration
Tests schema compatibility and data flow between silver PnL and gold top traders
"""

import sys
import os
sys.path.append('/home/jgupdogg/dev/claude_pipeline/dags')

def test_schema_compatibility():
    """Test that schemas are compatible between silver and gold layers"""
    print("🔍 Testing Silver to Gold Schema Compatibility...")
    
    try:
        # Test imports
        from silver_wallet_pnl_dag import get_silver_pnl_schema
        from gold_top_traders_dag import get_gold_top_traders_schema, GoldConfig
        
        print("✅ Successfully imported both DAG modules")
        
        # Test schema creation
        silver_schema = get_silver_pnl_schema()
        gold_schema = get_gold_top_traders_schema()
        
        print(f"✅ Silver schema: {len(silver_schema.fields)} fields")
        print(f"✅ Gold schema: {len(gold_schema.fields)} fields")
        
        # Check for required gold processing fields in silver
        silver_fields = [f.name for f in silver_schema.fields]
        required_gold_fields = [
            'processed_for_gold', 
            'gold_processed_at', 
            'gold_processing_status', 
            'gold_batch_id'
        ]
        
        print("\n🔍 Checking gold processing fields in silver schema:")
        for field in required_gold_fields:
            if field in silver_fields:
                print(f"  ✅ {field}")
            else:
                print(f"  ❌ Missing: {field}")
                return False
        
        # Check for required source fields that gold needs from silver
        required_source_fields = [
            'wallet_address', 'total_pnl', 'roi', 'win_rate', 'trade_count',
            'realized_pnl', 'unrealized_pnl', 'token_address', 'time_period'
        ]
        
        print("\n🔍 Checking required source fields in silver schema:")
        for field in required_source_fields:
            if field in silver_fields:
                print(f"  ✅ {field}")
            else:
                print(f"  ❌ Missing: {field}")
                return False
        
        # Test configuration
        config = GoldConfig()
        print(f"\n🔍 Gold configuration:")
        print(f"  Min PnL: ${config.min_total_pnl:,.2f}")
        print(f"  Min ROI: {config.min_roi}%")
        print(f"  Min Win Rate: {config.min_win_rate}%")
        print(f"  Min Trades: {config.min_trade_count}")
        print(f"  Max Traders/Batch: {config.max_traders_per_batch}")
        
        print("\n✅ Schema compatibility test PASSED")
        return True
        
    except Exception as e:
        print(f"❌ Schema compatibility test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_flow_logic():
    """Test the data flow logic conceptually"""
    print("\n🔍 Testing Data Flow Logic...")
    
    try:
        # Simulate the selection criteria
        mock_silver_records = [
            {
                'wallet_address': 'wallet1',
                'total_pnl': 50000,
                'roi': 75.0,
                'win_rate': 65.0,
                'trade_count': 25,
                'token_address': 'ALL_TOKENS',
                'time_period': 'all',
                'processed_for_gold': False
            },
            {
                'wallet_address': 'wallet2', 
                'total_pnl': 5000,
                'roi': 15.0,  # Below min ROI
                'win_rate': 80.0,
                'trade_count': 30,
                'token_address': 'ALL_TOKENS',
                'time_period': 'all',
                'processed_for_gold': False
            },
            {
                'wallet_address': 'wallet3',
                'total_pnl': 150000,
                'roi': 120.0,
                'win_rate': 85.0,
                'trade_count': 60,
                'token_address': 'ALL_TOKENS',
                'time_period': 'all',
                'processed_for_gold': False
            },
            {
                'wallet_address': 'wallet4',
                'total_pnl': 25000,
                'roi': 45.0,
                'win_rate': 72.0,
                'trade_count': 18,
                'token_address': 'ALL_TOKENS',
                'time_period': 'all',
                'processed_for_gold': True  # Already processed
            }
        ]
        
        from gold_top_traders_dag import GoldConfig
        config = GoldConfig()
        
        # Test filtering logic
        qualifying_wallets = []
        for record in mock_silver_records:
            if (record['processed_for_gold'] == False and
                record['token_address'] == 'ALL_TOKENS' and
                record['time_period'] == 'all' and
                record['total_pnl'] >= config.min_total_pnl and
                record['roi'] >= config.min_roi and
                record['win_rate'] >= config.min_win_rate and
                record['trade_count'] >= config.min_trade_count and
                record['total_pnl'] > 0):
                
                # Determine performance tier
                if (record['total_pnl'] >= 100000 and 
                    record['roi'] >= 100 and 
                    record['win_rate'] >= 80 and 
                    record['trade_count'] >= 50):
                    tier = "elite"
                elif (record['total_pnl'] >= 10000 and 
                      record['roi'] >= 50 and 
                      record['win_rate'] >= 70 and 
                      record['trade_count'] >= 20):
                    tier = "strong"
                else:
                    tier = "promising"
                
                qualifying_wallets.append({
                    'wallet': record['wallet_address'],
                    'tier': tier,
                    'pnl': record['total_pnl'],
                    'roi': record['roi']
                })
        
        print(f"📊 Mock data results:")
        print(f"  Total records: {len(mock_silver_records)}")
        print(f"  Qualifying wallets: {len(qualifying_wallets)}")
        
        for wallet in qualifying_wallets:
            print(f"    {wallet['wallet']}: {wallet['tier']} tier (PnL: ${wallet['pnl']:,.0f}, ROI: {wallet['roi']}%)")
        
        # Expected: wallet1 (promising), wallet3 (elite)
        # wallet2 fails ROI filter, wallet4 already processed
        expected_count = 2
        if len(qualifying_wallets) == expected_count:
            print(f"✅ Expected {expected_count} qualifying wallets, got {len(qualifying_wallets)}")
        else:
            print(f"❌ Expected {expected_count} qualifying wallets, got {len(qualifying_wallets)}")
            return False
        
        print("\n✅ Data flow logic test PASSED")
        return True
        
    except Exception as e:
        print(f"❌ Data flow logic test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all integration tests"""
    print("🚀 Silver to Gold Integration Test Suite")
    print("=" * 50)
    
    success = True
    
    # Test schema compatibility
    if not test_schema_compatibility():
        success = False
    
    # Test data flow logic
    if not test_data_flow_logic():
        success = False
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 ALL TESTS PASSED - Integration ready!")
        print("\nNext steps:")
        print("1. Deploy both DAGs to Airflow")
        print("2. Ensure silver PnL DAG runs first")
        print("3. Gold top traders DAG will process unprocessed records")
        print("4. Monitor processing state fields for proper tracking")
    else:
        print("❌ TESTS FAILED - Fix issues before deployment")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)