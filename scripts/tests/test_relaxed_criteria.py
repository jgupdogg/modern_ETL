#!/usr/bin/env python3
"""
Test relaxed gold criteria with mock data to verify logic
"""

def test_relaxed_criteria():
    """Test if relaxed criteria would find qualifying wallets"""
    print("🔍 Testing Relaxed Gold Criteria...")
    
    # Mock silver PnL data representing different wallet performance levels
    mock_wallets = [
        {
            'wallet_address': 'wallet_1',
            'total_pnl': 5000,
            'roi': 75.0,
            'win_rate': 65.0,
            'trade_count': 15,
            'token_address': 'ALL_TOKENS',
            'time_period': 'all',
            'processed_for_gold': False
        },
        {
            'wallet_address': 'wallet_2',
            'total_pnl': 500,
            'roi': 15.0,
            'win_rate': 55.0,
            'trade_count': 8,
            'token_address': 'ALL_TOKENS',
            'time_period': 'all',
            'processed_for_gold': False
        },
        {
            'wallet_address': 'wallet_3',
            'total_pnl': 50,
            'roi': 5.0,
            'win_rate': 45.0,
            'trade_count': 3,
            'token_address': 'ALL_TOKENS',
            'time_period': 'all',
            'processed_for_gold': False
        },
        {
            'wallet_address': 'wallet_4',
            'total_pnl': -200,
            'roi': -10.0,
            'win_rate': 30.0,
            'trade_count': 12,
            'token_address': 'ALL_TOKENS',
            'time_period': 'all',
            'processed_for_gold': False
        },
        {
            'wallet_address': 'wallet_5',
            'total_pnl': 2000,
            'roi': 40.0,
            'win_rate': 68.0,
            'trade_count': 12,
            'token_address': 'ALL_TOKENS',
            'time_period': 'all',
            'processed_for_gold': False
        }
    ]
    
    # Relaxed criteria
    min_total_pnl = 100.0
    min_roi = 10.0
    min_win_rate = 50.0
    min_trade_count = 5
    
    print(f"📊 Relaxed Criteria:")
    print(f"  Min PnL: ${min_total_pnl}")
    print(f"  Min ROI: {min_roi}%")
    print(f"  Min Win Rate: {min_win_rate}%")
    print(f"  Min Trades: {min_trade_count}")
    
    qualifying_wallets = []
    
    for wallet in mock_wallets:
        if (wallet['processed_for_gold'] == False and
            wallet['token_address'] == 'ALL_TOKENS' and
            wallet['time_period'] == 'all' and
            wallet['total_pnl'] >= min_total_pnl and
            wallet['roi'] >= min_roi and
            wallet['win_rate'] >= min_win_rate and
            wallet['trade_count'] >= min_trade_count and
            wallet['total_pnl'] > 0):
            
            # Determine performance tier
            if (wallet['total_pnl'] >= 10000 and 
                wallet['roi'] >= 50 and 
                wallet['win_rate'] >= 70 and 
                wallet['trade_count'] >= 20):
                tier = "elite"
            elif (wallet['total_pnl'] >= 1000 and 
                  wallet['roi'] >= 25 and 
                  wallet['win_rate'] >= 60 and 
                  wallet['trade_count'] >= 10):
                tier = "strong"
            else:
                tier = "promising"
            
            qualifying_wallets.append({
                'wallet': wallet['wallet_address'],
                'tier': tier,
                'pnl': wallet['total_pnl'],
                'roi': wallet['roi'],
                'win_rate': wallet['win_rate'],
                'trades': wallet['trade_count']
            })
    
    print(f"\n🎯 Results:")
    print(f"  Total test wallets: {len(mock_wallets)}")
    print(f"  Qualifying wallets: {len(qualifying_wallets)}")
    
    if qualifying_wallets:
        print(f"\n✅ Qualifying Wallets:")
        for wallet in qualifying_wallets:
            print(f"  🏆 {wallet['wallet']}: {wallet['tier']} tier")
            print(f"     PnL: ${wallet['pnl']:,.0f}, ROI: {wallet['roi']}%, Win Rate: {wallet['win_rate']}%, Trades: {wallet['trades']}")
    else:
        print(f"\n❌ No qualifying wallets found")
        return False
    
    return True

def main():
    """Run relaxed criteria test"""
    print("🚀 Relaxed Gold Criteria Test")
    print("=" * 50)
    
    success = test_relaxed_criteria()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ Relaxed criteria should find qualifying wallets")
        print("\n💡 Next Steps:")
        print("1. Trigger silver wallet PnL DAG to generate data with new schema")
        print("2. Trigger gold top traders DAG to test end-to-end flow")
        print("3. Verify new records are created in gold layer")
    else:
        print("❌ Even relaxed criteria too stringent")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)