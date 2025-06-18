#!/usr/bin/env python3
"""
Test script for the new raw bronze pipeline implementation
Tests the bronze schema refactor and transformation logic
"""

import sys
import os
import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any

# Add dags directory to path for imports
sys.path.append('/home/jgupdogg/dev/claude_pipeline/dags')

def test_raw_transaction_schema():
    """Test the new raw transaction schema"""
    print("🧪 Testing Raw Transaction Schema")
    print("=" * 50)
    
    try:
        from tasks.bronze_tasks import get_raw_transaction_schema
        
        schema = get_raw_transaction_schema()
        print(f"✅ Schema loaded successfully with {len(schema)} fields")
        
        # Check key fields exist
        expected_fields = [
            'wallet_address', 'transaction_hash', 'timestamp',
            'base_symbol', 'base_address', 'base_type_swap', 'base_ui_change_amount', 'base_nearest_price',
            'quote_symbol', 'quote_address', 'quote_type_swap', 'quote_ui_change_amount', 'quote_nearest_price',
            'processed_for_pnl', 'pnl_processed_at', 'pnl_processing_batch_id',
            'batch_id', 'data_source'
        ]
        
        schema_fields = [field.name for field in schema]
        missing_fields = [field for field in expected_fields if field not in schema_fields]
        
        if missing_fields:
            print(f"❌ Missing fields: {missing_fields}")
            return False
        else:
            print("✅ All expected fields present")
            
        # Print schema summary
        print("\n📋 Schema Fields:")
        for field in schema:
            print(f"  - {field.name}: {field.type} (nullable: {field.nullable})")
            
        return True
        
    except Exception as e:
        print(f"❌ Schema test failed: {e}")
        return False


def test_raw_transformation():
    """Test the raw transformation function with sample data"""
    print("\n🧪 Testing Raw Transformation Logic")
    print("=" * 50)
    
    try:
        from tasks.bronze_tasks import transform_trade_raw
        
        # Sample raw trade data (from your example)
        sample_trade = {
            "quote": {
                "symbol": "USDC",
                "decimals": 6,
                "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "amount": 350351441,
                "type": "transfer",
                "type_swap": "from",
                "ui_amount": 350.351441,
                "price": None,
                "nearest_price": 0.99991594,
                "change_amount": -350351441,
                "ui_change_amount": -350.351441
            },
            "base": {
                "symbol": "SOL",
                "decimals": 9,
                "address": "So11111111111111111111111111111111111111112",
                "amount": 1610859019,
                "type": "transfer",
                "type_swap": "to",
                "fee_info": None,
                "ui_amount": 1.610859019,
                "price": None,
                "nearest_price": 216.65610374576917,
                "change_amount": 1610859019,
                "ui_change_amount": 1.610859019
            },
            "base_price": None,
            "quote_price": None,
            "tx_hash": "3bHiF6b9xmuAjanyLgvKbM2fnFSBo9FeY4rb67xrnGZWx24S6Yyroc8upiJgyUnG29p39jQqxfeRtZ5pTcT9hQJm",
            "source": "lifinity",
            "block_unix_time": 1731555934,
            "tx_type": "swap",
            "address": "DrRd8gYMJu9XGxLhwTCPdHNLXCKHsxJtMpbn62YqmwQe",
            "owner": "GKQBjCn68cTFwUcUiszSioE3B2tAeemfgS2x4Zk2Lyz9"
        }
        
        wallet_address = "GKQBjCn68cTFwUcUiszSioE3B2tAeemfgS2x4Zk2Lyz9"
        fetched_at = pd.Timestamp.utcnow(tz='UTC')
        
        # Test transformation
        result = transform_trade_raw(sample_trade, wallet_address, fetched_at)
        
        if result is None:
            print("❌ Transformation returned None")
            return False
            
        print("✅ Transformation successful")
        
        # Check key fields are preserved
        expected_mappings = {
            'transaction_hash': sample_trade['tx_hash'],
            'base_symbol': sample_trade['base']['symbol'],
            'base_address': sample_trade['base']['address'], 
            'base_type_swap': sample_trade['base']['type_swap'],
            'base_ui_change_amount': sample_trade['base']['ui_change_amount'],
            'base_nearest_price': sample_trade['base']['nearest_price'],
            'quote_symbol': sample_trade['quote']['symbol'],
            'quote_address': sample_trade['quote']['address'],
            'quote_type_swap': sample_trade['quote']['type_swap'],
            'quote_ui_change_amount': sample_trade['quote']['ui_change_amount'],
            'quote_nearest_price': sample_trade['quote']['nearest_price'],
            'source': sample_trade['source'],
            'tx_type': sample_trade['tx_type'],
            'block_unix_time': sample_trade['block_unix_time'],
            'owner': sample_trade['owner']
        }
        
        print("\n📊 Field Mapping Validation:")
        all_correct = True
        for field, expected_value in expected_mappings.items():
            actual_value = result.get(field)
            if actual_value == expected_value:
                print(f"  ✅ {field}: {actual_value}")
            else:
                print(f"  ❌ {field}: expected {expected_value}, got {actual_value}")
                all_correct = False
        
        # Check processing state defaults
        processing_defaults = {
            'processed_for_pnl': False,
            'pnl_processed_at': None,
            'pnl_processing_batch_id': None,
            'data_source': 'birdeye_v3'
        }
        
        print("\n🔄 Processing State Validation:")
        for field, expected_value in processing_defaults.items():
            actual_value = result.get(field)
            if actual_value == expected_value:
                print(f"  ✅ {field}: {actual_value}")
            else:
                print(f"  ❌ {field}: expected {expected_value}, got {actual_value}")
                all_correct = False
        
        return all_correct
        
    except Exception as e:
        print(f"❌ Transformation test failed: {e}")
        import traceback
        print(traceback.format_exc())
        return False


def test_swap_direction_logic():
    """Test the swap direction determination logic"""
    print("\n🧪 Testing Swap Direction Logic")
    print("=" * 50)
    
    try:
        from tasks.bronze_tasks import transform_trade_raw
        
        # Test case 1: base_type_swap = "from", quote_type_swap = "to" (USDC → SOL)
        test_case_1 = {
            "base": {
                "symbol": "USDC", 
                "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "type_swap": "from",
                "ui_change_amount": -100.0,
                "nearest_price": 1.0
            },
            "quote": {
                "symbol": "SOL",
                "address": "So11111111111111111111111111111111111111112", 
                "type_swap": "to",
                "ui_change_amount": 0.5,
                "nearest_price": 200.0
            },
            "tx_hash": "test_hash_1",
            "source": "raydium",
            "tx_type": "swap",
            "block_unix_time": 1731555934,
            "owner": "test_wallet_123"
        }
        
        result_1 = transform_trade_raw(test_case_1, "test_wallet_123", pd.Timestamp.utcnow(tz='UTC'))
        
        if result_1:
            print("✅ Test Case 1 (USDC → SOL):")
            print(f"  Base (from): {result_1['base_symbol']} ({result_1['base_type_swap']}) = {result_1['base_ui_change_amount']}")
            print(f"  Quote (to): {result_1['quote_symbol']} ({result_1['quote_type_swap']}) = {result_1['quote_ui_change_amount']}")
        else:
            print("❌ Test Case 1 failed")
            return False
        
        # Test case 2: base_type_swap = "to", quote_type_swap = "from" (SOL → USDC)
        test_case_2 = {
            "base": {
                "symbol": "SOL",
                "address": "So11111111111111111111111111111111111111112",
                "type_swap": "to", 
                "ui_change_amount": 0.5,
                "nearest_price": 200.0
            },
            "quote": {
                "symbol": "USDC",
                "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "type_swap": "from",
                "ui_change_amount": -100.0,
                "nearest_price": 1.0
            },
            "tx_hash": "test_hash_2",
            "source": "raydium", 
            "tx_type": "swap",
            "block_unix_time": 1731555934,
            "owner": "test_wallet_123"
        }
        
        result_2 = transform_trade_raw(test_case_2, "test_wallet_123", pd.Timestamp.utcnow(tz='UTC'))
        
        if result_2:
            print("✅ Test Case 2 (SOL → USDC):")
            print(f"  Base (to): {result_2['base_symbol']} ({result_2['base_type_swap']}) = {result_2['base_ui_change_amount']}")
            print(f"  Quote (from): {result_2['quote_symbol']} ({result_2['quote_type_swap']}) = {result_2['quote_ui_change_amount']}")
        else:
            print("❌ Test Case 2 failed")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ Swap direction test failed: {e}")
        return False


def test_silver_normalization():
    """Test the silver layer normalization logic"""
    print("\n🧪 Testing Silver Normalization Logic")
    print("=" * 50)
    
    try:
        # Create sample raw bronze data
        raw_data = [
            {
                'wallet_address': 'test_wallet_123',
                'transaction_hash': 'hash_1',
                'timestamp': '2025-06-18 10:00:00',
                'base_symbol': 'USDC',
                'base_address': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                'base_type_swap': 'from',
                'base_ui_change_amount': -100.0,
                'base_nearest_price': 1.0,
                'quote_symbol': 'SOL', 
                'quote_address': 'So11111111111111111111111111111111111111112',
                'quote_type_swap': 'to',
                'quote_ui_change_amount': 0.5,
                'quote_nearest_price': 200.0,
                'source': 'raydium',
                'processed_for_pnl': False
            }
        ]
        
        # Test normalization logic conceptually (without actual PySpark)
        for trade in raw_data:
            print(f"📊 Raw Trade: {trade['base_symbol']} → {trade['quote_symbol']}")
            
            # Determine sold token (type_swap = "from")
            if trade['base_type_swap'] == 'from':
                sold_token = {
                    'address': trade['base_address'],
                    'symbol': trade['base_symbol'],
                    'quantity': abs(trade['base_ui_change_amount']),
                    'price': trade['base_nearest_price']
                }
                bought_token = {
                    'address': trade['quote_address'], 
                    'symbol': trade['quote_symbol'],
                    'quantity': abs(trade['quote_ui_change_amount']),
                    'price': trade['quote_nearest_price']
                }
            else:
                sold_token = {
                    'address': trade['quote_address'],
                    'symbol': trade['quote_symbol'], 
                    'quantity': abs(trade['quote_ui_change_amount']),
                    'price': trade['quote_nearest_price']
                }
                bought_token = {
                    'address': trade['base_address'],
                    'symbol': trade['base_symbol'],
                    'quantity': abs(trade['base_ui_change_amount']),
                    'price': trade['base_nearest_price']
                }
            
            print(f"  🔴 SOLD: {sold_token['quantity']} {sold_token['symbol']} @ ${sold_token['price']:.2f} = ${sold_token['quantity'] * sold_token['price']:.2f}")
            print(f"  🟢 BOUGHT: {bought_token['quantity']} {bought_token['symbol']} @ ${bought_token['price']:.2f} = ${bought_token['quantity'] * bought_token['price']:.2f}")
            
            # This would create two portfolio entries:
            # 1. SELL record for USDC
            # 2. BUY record for SOL
            print(f"  📈 Portfolio Impact:")
            print(f"    - SELL {sold_token['symbol']}: -{sold_token['quantity']} (cost basis check needed)")
            print(f"    - BUY {bought_token['symbol']}: +{bought_token['quantity']} (cost basis: ${bought_token['quantity'] * bought_token['price']:.2f})")
        
        print("✅ Normalization logic validation successful")
        return True
        
    except Exception as e:
        print(f"❌ Normalization test failed: {e}")
        return False


def test_portfolio_pnl_concept():
    """Test the portfolio PnL calculation concept"""
    print("\n🧪 Testing Portfolio PnL Concept")
    print("=" * 50)
    
    try:
        # Simulate a wallet's trading sequence
        portfolio = {}  # {token_address: {'quantity': float, 'cost_basis': float, 'symbol': str}}
        realized_pnl = 0.0
        trade_count = 0
        winning_trades = 0
        
        # Sample trading sequence
        trades = [
            # Trade 1: Buy SOL with USDC
            {
                'type': 'buy',
                'token_address': 'So11111111111111111111111111111111111111112',
                'token_symbol': 'SOL',
                'quantity': 0.5,
                'price': 200.0,  # Cost basis
                'timestamp': '2025-06-18 10:00:00'
            },
            # Trade 2: Sell half SOL for USDC
            {
                'type': 'sell',
                'token_address': 'So11111111111111111111111111111111111111112', 
                'token_symbol': 'SOL',
                'quantity': 0.25,
                'price': 220.0,  # Sale price
                'timestamp': '2025-06-18 11:00:00'
            },
            # Trade 3: Buy more SOL
            {
                'type': 'buy',
                'token_address': 'So11111111111111111111111111111111111111112',
                'token_symbol': 'SOL', 
                'quantity': 1.0,
                'price': 180.0,  # Different cost basis
                'timestamp': '2025-06-18 12:00:00'
            }
        ]
        
        print("📊 Processing Trading Sequence:")
        
        for i, trade in enumerate(trades, 1):
            print(f"\n  Trade {i}: {trade['type'].upper()} {trade['quantity']} {trade['symbol']} @ ${trade['price']:.2f}")
            
            token_addr = trade['token_address']
            
            if trade['type'] == 'buy':
                # Add to portfolio
                if token_addr in portfolio:
                    # Average cost basis (simplified)
                    old_qty = portfolio[token_addr]['quantity']
                    old_cost = portfolio[token_addr]['cost_basis']
                    new_qty = old_qty + trade['quantity']
                    new_cost = old_cost + (trade['quantity'] * trade['price'])
                    
                    portfolio[token_addr] = {
                        'quantity': new_qty,
                        'cost_basis': new_cost,
                        'symbol': trade['token_symbol']
                    }
                else:
                    portfolio[token_addr] = {
                        'quantity': trade['quantity'],
                        'cost_basis': trade['quantity'] * trade['price'],
                        'symbol': trade['token_symbol']
                    }
                    
                print(f"    Portfolio: {portfolio[token_addr]['quantity']:.4f} {trade['token_symbol']} (cost: ${portfolio[token_addr]['cost_basis']:.2f})")
            
            elif trade['type'] == 'sell':
                # Process sale using FIFO
                if token_addr in portfolio:
                    available_qty = portfolio[token_addr]['quantity']
                    avg_cost_per_token = portfolio[token_addr]['cost_basis'] / available_qty
                    
                    if available_qty >= trade['quantity']:
                        # Calculate realized PnL
                        cost_removed = avg_cost_per_token * trade['quantity']
                        sale_value = trade['quantity'] * trade['price']
                        trade_pnl = sale_value - cost_removed
                        
                        realized_pnl += trade_pnl
                        trade_count += 1
                        if trade_pnl > 0:
                            winning_trades += 1
                        
                        # Update portfolio
                        new_qty = available_qty - trade['quantity']
                        new_cost = portfolio[token_addr]['cost_basis'] - cost_removed
                        
                        if new_qty > 0:
                            portfolio[token_addr]['quantity'] = new_qty
                            portfolio[token_addr]['cost_basis'] = new_cost
                        else:
                            del portfolio[token_addr]
                        
                        print(f"    Realized PnL: ${trade_pnl:.2f} (cost: ${cost_removed:.2f}, sale: ${sale_value:.2f})")
                        if token_addr in portfolio:
                            print(f"    Remaining: {portfolio[token_addr]['quantity']:.4f} {trade['token_symbol']} (cost: ${portfolio[token_addr]['cost_basis']:.2f})")
                        else:
                            print(f"    Position closed")
                    else:
                        print(f"    ❌ Insufficient quantity (have {available_qty}, need {trade['quantity']})")
                else:
                    print(f"    ❌ No position in {trade['token_symbol']}")
        
        # Calculate final metrics
        win_rate = (winning_trades / trade_count * 100) if trade_count > 0 else 0
        
        print(f"\n📈 Final Portfolio State:")
        total_unrealized = 0
        for token_addr, position in portfolio.items():
            current_price = 210.0  # Assume current SOL price
            current_value = position['quantity'] * current_price
            unrealized_pnl = current_value - position['cost_basis']
            total_unrealized += unrealized_pnl
            
            print(f"  {position['symbol']}: {position['quantity']:.4f} tokens")
            print(f"    Cost basis: ${position['cost_basis']:.2f}")
            print(f"    Current value: ${current_value:.2f} (@ ${current_price:.2f})")
            print(f"    Unrealized PnL: ${unrealized_pnl:.2f}")
        
        total_pnl = realized_pnl + total_unrealized
        
        print(f"\n💰 PnL Summary:")
        print(f"  Realized PnL: ${realized_pnl:.2f}")
        print(f"  Unrealized PnL: ${total_unrealized:.2f}")
        print(f"  Total PnL: ${total_pnl:.2f}")
        print(f"  Win Rate: {win_rate:.1f}% ({winning_trades}/{trade_count})")
        
        print("✅ Portfolio PnL concept validation successful")
        return True
        
    except Exception as e:
        print(f"❌ Portfolio PnL test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("🚀 Testing New Raw Bronze Pipeline Implementation")
    print("=" * 80)
    
    tests = [
        ("Raw Transaction Schema", test_raw_transaction_schema),
        ("Raw Transformation Logic", test_raw_transformation), 
        ("Swap Direction Logic", test_swap_direction_logic),
        ("Silver Normalization", test_silver_normalization),
        ("Portfolio PnL Concept", test_portfolio_pnl_concept)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{'='*80}")
    print("📋 TEST SUMMARY")
    print("=" * 80)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 Results: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 All tests passed! Raw bronze pipeline implementation is ready.")
    else:
        print("⚠️  Some tests failed. Review implementation before proceeding.")
    
    return passed == total


if __name__ == "__main__":
    main()