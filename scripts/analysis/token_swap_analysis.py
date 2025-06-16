#!/usr/bin/env python3
"""
Token Swap Analysis Script

Focused analysis of token swaps, trading patterns, and DEX activity
from the webhook data to provide actionable trading insights.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Any, Tuple, Set

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TokenSwapAnalyzer:
    def __init__(self, data_dir: str = "/home/jgupdogg/dev/claude_pipeline/data"):
        self.data_dir = data_dir
        self.webhook_dir = os.path.join(data_dir, "webhooks")
        
        # Known token addresses for better analysis
        self.known_tokens = {
            "So11111111111111111111111111111111111111112": "SOL",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
            "HnNLd1tG4XgH9YkahSkud68ew6vrryh7eZFkusyzcHgN": "WIF",
            "BGywaqWmaTbRitLmNVsGXGY2sd5XEP6oqrUD9nA8MYet": "bonk",
            "4vBiGz4ymPXkhvRBQopSB87zwTA76sUsSAo9JpqkJG1y": "TREMP"
        }
        
        # DEX mappings
        self.dex_info = {
            "RAYDIUM": {"name": "Raydium", "type": "AMM"},
            "JUPITER": {"name": "Jupiter", "type": "Aggregator"},
            "PUMP_AMM": {"name": "Pump.fun AMM", "type": "Memecoin Platform"},
            "PUMP_FUN": {"name": "Pump.fun", "type": "Memecoin Platform"},
            "ORCA": {"name": "Orca", "type": "AMM"},
            "SERUM": {"name": "Serum", "type": "Orderbook"}
        }
        
    def analyze_trading_patterns(self, hours_back: int = 24, sample_size: int = 1000) -> Dict[str, Any]:
        """Analyze recent trading patterns and hot tokens"""
        logger.info(f"Analyzing trading patterns for the last {hours_back} hours...")
        
        # Get recent files
        recent_files = self._get_recent_files(hours_back)
        if len(recent_files) > sample_size:
            recent_files = recent_files[-sample_size:]  # Take most recent
            
        trading_analysis = {
            "time_window": f"Last {hours_back} hours",
            "files_analyzed": len(recent_files),
            "hot_tokens": defaultdict(lambda: {
                "symbol": "Unknown",
                "swap_count": 0,
                "total_volume_sol": 0.0,
                "unique_traders": set(),
                "avg_swap_size": 0.0,
                "price_movements": [],
                "dex_distribution": Counter()
            }),
            "dex_activity": Counter(),
            "trader_activity": defaultdict(lambda: {
                "swap_count": 0,
                "tokens_traded": set(),
                "total_volume": 0.0,
                "preferred_dex": Counter()
            }),
            "swap_patterns": {
                "total_swaps": 0,
                "sol_pairs": 0,
                "stablecoin_pairs": 0,
                "memecoin_activity": 0,
                "large_swaps": 0  # > 10 SOL
            }
        }
        
        # Analyze each file
        for file_path in recent_files:
            try:
                self._analyze_trading_file(file_path, trading_analysis)
            except Exception as e:
                logger.warning(f"Error processing {file_path}: {e}")
                
        # Post-process analysis
        self._finalize_trading_analysis(trading_analysis)
        
        return trading_analysis
    
    def _get_recent_files(self, hours_back: int) -> List[str]:
        """Get webhook files from the last N hours"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        recent_files = []
        
        for root, dirs, files in os.walk(self.webhook_dir):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    if file_time >= cutoff_time:
                        recent_files.append(file_path)
                        
        # Sort by modification time
        recent_files.sort(key=lambda x: os.path.getmtime(x))
        return recent_files
    
    def _analyze_trading_file(self, file_path: str, analysis: Dict):
        """Analyze a single webhook file for trading patterns"""
        with open(file_path, 'r') as f:
            webhook_data = json.load(f)
            
        payload = webhook_data.get('payload', [])
        if not isinstance(payload, list):
            payload = [payload]
            
        for tx in payload:
            if not isinstance(tx, dict) or tx.get('type') != 'SWAP':
                continue
                
            self._process_swap_transaction(tx, analysis)
    
    def _process_swap_transaction(self, tx: Dict, analysis: Dict):
        """Process a single swap transaction"""
        analysis["swap_patterns"]["total_swaps"] += 1
        
        # Extract swap details
        source = tx.get('source', 'UNKNOWN')
        signature = tx.get('signature', '')
        token_transfers = tx.get('tokenTransfers', [])
        
        analysis["dex_activity"][source] += 1
        
        if len(token_transfers) < 2:
            return
            
        # Identify swap pairs
        from_token = token_transfers[0].get('mint', '')
        to_token = token_transfers[-1].get('mint', '')
        from_amount = token_transfers[0].get('tokenAmount', 0)
        to_amount = token_transfers[-1].get('tokenAmount', 0)
        trader = token_transfers[0].get('fromUserAccount', '')
        
        # Classify swap types
        sol_mint = "So11111111111111111111111111111111111111112"
        stablecoins = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
        
        if from_token == sol_mint or to_token == sol_mint:
            analysis["swap_patterns"]["sol_pairs"] += 1
            
        if from_token in stablecoins or to_token in stablecoins:
            analysis["swap_patterns"]["stablecoin_pairs"] += 1
            
        # Check for memecoin activity (tokens ending with "pump")
        if from_token.endswith("pump") or to_token.endswith("pump"):
            analysis["swap_patterns"]["memecoin_activity"] += 1
            
        # Track large swaps (>10 SOL equivalent)
        if (from_token == sol_mint and from_amount > 10) or (to_token == sol_mint and to_amount > 10):
            analysis["swap_patterns"]["large_swaps"] += 1
            
        # Update token statistics
        tokens_to_track = [from_token, to_token]
        for token in tokens_to_track:
            if token:
                token_data = analysis["hot_tokens"][token]
                token_data["symbol"] = self.known_tokens.get(token, token[:8] + "...")
                token_data["swap_count"] += 1
                token_data["unique_traders"].add(trader)
                token_data["dex_distribution"][source] += 1
                
                # Estimate SOL volume
                if token == sol_mint:
                    if from_token == sol_mint:
                        token_data["total_volume_sol"] += from_amount
                    else:
                        token_data["total_volume_sol"] += to_amount
                        
        # Update trader statistics
        if trader:
            trader_data = analysis["trader_activity"][trader]
            trader_data["swap_count"] += 1
            trader_data["tokens_traded"].add(from_token)
            trader_data["tokens_traded"].add(to_token)
            trader_data["preferred_dex"][source] += 1
            
            # Estimate trader volume
            if from_token == sol_mint:
                trader_data["total_volume"] += from_amount
            elif to_token == sol_mint:
                trader_data["total_volume"] += to_amount
    
    def _finalize_trading_analysis(self, analysis: Dict):
        """Finalize and clean up the trading analysis"""
        # Convert sets to counts and calculate averages
        for token, data in analysis["hot_tokens"].items():
            data["unique_traders"] = len(data["unique_traders"])
            if data["swap_count"] > 0:
                data["avg_swap_size"] = data["total_volume_sol"] / data["swap_count"]
                
        for trader, data in analysis["trader_activity"].items():
            data["tokens_traded"] = len(data["tokens_traded"])
            
        # Sort hot tokens by activity
        sorted_tokens = sorted(
            analysis["hot_tokens"].items(),
            key=lambda x: (x[1]["swap_count"], x[1]["total_volume_sol"]),
            reverse=True
        )
        analysis["hot_tokens"] = dict(sorted_tokens[:20])  # Top 20
        
        # Sort active traders
        sorted_traders = sorted(
            analysis["trader_activity"].items(),
            key=lambda x: (x[1]["swap_count"], x[1]["total_volume"]),
            reverse=True
        )
        analysis["trader_activity"] = dict(sorted_traders[:10])  # Top 10
    
    def analyze_memecoin_trends(self, sample_size: int = 500) -> Dict[str, Any]:
        """Analyze memecoin trading trends and pump.fun activity"""
        logger.info("Analyzing memecoin trends...")
        
        # Get recent files
        recent_files = self._get_recent_files(24)  # Last 24 hours
        if len(recent_files) > sample_size:
            recent_files = recent_files[-sample_size:]
            
        memecoin_analysis = {
            "pump_fun_activity": {
                "total_swaps": 0,
                "unique_tokens": set(),
                "unique_traders": set(),
                "total_volume_sol": 0.0
            },
            "trending_memecoins": defaultdict(lambda: {
                "symbol": "Unknown",
                "swap_count": 0,
                "traders": set(),
                "volume_sol": 0.0,
                "recent_swaps": []
            }),
            "trader_patterns": {
                "memecoin_only_traders": set(),
                "cross_platform_traders": set(),
                "high_volume_memecoin_traders": set()
            }
        }
        
        # Analyze files for memecoin activity
        for file_path in recent_files:
            try:
                self._analyze_memecoin_file(file_path, memecoin_analysis)
            except Exception as e:
                logger.warning(f"Error processing {file_path}: {e}")
                
        # Finalize memecoin analysis
        self._finalize_memecoin_analysis(memecoin_analysis)
        
        return memecoin_analysis
    
    def _analyze_memecoin_file(self, file_path: str, analysis: Dict):
        """Analyze a file for memecoin trading activity"""
        with open(file_path, 'r') as f:
            webhook_data = json.load(f)
            
        payload = webhook_data.get('payload', [])
        if not isinstance(payload, list):
            payload = [payload]
            
        for tx in payload:
            if not isinstance(tx, dict) or tx.get('type') != 'SWAP':
                continue
                
            source = tx.get('source', '')
            token_transfers = tx.get('tokenTransfers', [])
            
            # Focus on pump.fun and memecoin activity
            is_pump_fun = source in ['PUMP_AMM', 'PUMP_FUN']
            has_pump_token = any(transfer.get('mint', '').endswith('pump') for transfer in token_transfers)
            
            if is_pump_fun or has_pump_token:
                self._process_memecoin_swap(tx, analysis, is_pump_fun)
    
    def _process_memecoin_swap(self, tx: Dict, analysis: Dict, is_pump_fun: bool):
        """Process a memecoin swap transaction"""
        token_transfers = tx.get('tokenTransfers', [])
        if len(token_transfers) < 2:
            return
            
        trader = token_transfers[0].get('fromUserAccount', '')
        timestamp = tx.get('timestamp', 0)
        
        if is_pump_fun:
            analysis["pump_fun_activity"]["total_swaps"] += 1
            if trader:
                analysis["pump_fun_activity"]["unique_traders"].add(trader)
                
        # Track memecoin tokens
        for transfer in token_transfers:
            mint = transfer.get('mint', '')
            amount = transfer.get('tokenAmount', 0)
            
            if mint.endswith('pump') or is_pump_fun:
                token_data = analysis["trending_memecoins"][mint]
                token_data["symbol"] = mint[:8] + "..."
                token_data["swap_count"] += 1
                if trader:
                    token_data["traders"].add(trader)
                    
                # Track SOL volume
                if mint == "So11111111111111111111111111111111111111112":
                    token_data["volume_sol"] += amount
                    analysis["pump_fun_activity"]["total_volume_sol"] += amount
                    
                token_data["recent_swaps"].append({
                    "timestamp": timestamp,
                    "amount": amount,
                    "trader": trader[:8] + "..." if trader else "Unknown"
                })
                
                analysis["pump_fun_activity"]["unique_tokens"].add(mint)
    
    def _finalize_memecoin_analysis(self, analysis: Dict):
        """Finalize memecoin analysis"""
        # Convert sets to counts
        analysis["pump_fun_activity"]["unique_tokens"] = len(analysis["pump_fun_activity"]["unique_tokens"])
        analysis["pump_fun_activity"]["unique_traders"] = len(analysis["pump_fun_activity"]["unique_traders"])
        
        for token, data in analysis["trending_memecoins"].items():
            data["traders"] = len(data["traders"])
            data["recent_swaps"] = data["recent_swaps"][-5:]  # Keep last 5 swaps
            
        # Sort by activity
        sorted_memecoins = sorted(
            analysis["trending_memecoins"].items(),
            key=lambda x: (x[1]["swap_count"], x[1]["volume_sol"]),
            reverse=True
        )
        analysis["trending_memecoins"] = dict(sorted_memecoins[:15])  # Top 15
    
    def generate_trading_insights(self, trading_analysis: Dict, memecoin_analysis: Dict) -> Dict[str, Any]:
        """Generate actionable trading insights"""
        insights = {
            "market_overview": self._get_market_overview(trading_analysis),
            "hot_opportunities": self._identify_hot_opportunities(trading_analysis),
            "memecoin_alerts": self._get_memecoin_alerts(memecoin_analysis),
            "dex_recommendations": self._get_dex_recommendations(trading_analysis),
            "risk_signals": self._identify_risk_signals(trading_analysis, memecoin_analysis),
            "whale_activity": self._detect_whale_activity(trading_analysis)
        }
        
        return insights
    
    def _get_market_overview(self, analysis: Dict) -> Dict[str, Any]:
        """Generate market overview"""
        total_swaps = analysis["swap_patterns"]["total_swaps"]
        
        return {
            "total_activity": total_swaps,
            "sol_dominance": analysis["swap_patterns"]["sol_pairs"] / total_swaps if total_swaps > 0 else 0,
            "memecoin_share": analysis["swap_patterns"]["memecoin_activity"] / total_swaps if total_swaps > 0 else 0,
            "large_swap_ratio": analysis["swap_patterns"]["large_swaps"] / total_swaps if total_swaps > 0 else 0,
            "most_active_dex": analysis["dex_activity"].most_common(1)[0] if analysis["dex_activity"] else ("None", 0)
        }
    
    def _identify_hot_opportunities(self, analysis: Dict) -> List[Dict[str, Any]]:
        """Identify hot trading opportunities"""
        opportunities = []
        
        for token, data in list(analysis["hot_tokens"].items())[:5]:
            if data["swap_count"] >= 5:  # Minimum activity threshold
                opportunity = {
                    "token": data["symbol"],
                    "mint": token,
                    "activity_score": data["swap_count"],
                    "unique_traders": data["unique_traders"],
                    "avg_swap_size": round(data["avg_swap_size"], 4),
                    "primary_dex": data["dex_distribution"].most_common(1)[0][0] if data["dex_distribution"] else "Unknown",
                    "volume_sol": round(data["total_volume_sol"], 2)
                }
                opportunities.append(opportunity)
                
        return opportunities
    
    def _get_memecoin_alerts(self, analysis: Dict) -> List[Dict[str, Any]]:
        """Generate memecoin trading alerts"""
        alerts = []
        
        for token, data in list(analysis["trending_memecoins"].items())[:5]:
            if data["swap_count"] >= 3:  # Minimum activity for alert
                alert = {
                    "token": data["symbol"],
                    "mint": token,
                    "swap_count": data["swap_count"],
                    "trader_count": data["traders"],
                    "volume_sol": round(data["volume_sol"], 2),
                    "recent_activity": len(data["recent_swaps"]),
                    "alert_level": "HIGH" if data["swap_count"] > 10 else "MEDIUM"
                }
                alerts.append(alert)
                
        return alerts
    
    def _get_dex_recommendations(self, analysis: Dict) -> Dict[str, Any]:
        """Get DEX usage recommendations"""
        top_dexes = analysis["dex_activity"].most_common(3)
        
        recommendations = {
            "most_liquid": top_dexes[0] if top_dexes else ("None", 0),
            "best_for_memecoins": "PUMP_AMM" if "PUMP_AMM" in analysis["dex_activity"] else "RAYDIUM",
            "arbitrage_opportunities": len([dex for dex, count in top_dexes if count > 10])
        }
        
        return recommendations
    
    def _identify_risk_signals(self, trading_analysis: Dict, memecoin_analysis: Dict) -> List[str]:
        """Identify potential risk signals"""
        signals = []
        
        # High memecoin activity could indicate bubble
        total_swaps = trading_analysis["swap_patterns"]["total_swaps"]
        memecoin_ratio = trading_analysis["swap_patterns"]["memecoin_activity"] / total_swaps if total_swaps > 0 else 0
        
        if memecoin_ratio > 0.7:
            signals.append("HIGH_MEMECOIN_ACTIVITY: >70% of swaps are memecoins - potential bubble risk")
            
        # Large swap concentration
        if trading_analysis["swap_patterns"]["large_swaps"] / total_swaps > 0.3 if total_swaps > 0 else False:
            signals.append("WHALE_CONCENTRATION: High percentage of large swaps detected")
            
        # Single DEX dominance
        if trading_analysis["dex_activity"]:
            top_dex_share = trading_analysis["dex_activity"].most_common(1)[0][1] / total_swaps if total_swaps > 0 else 0
            if top_dex_share > 0.8:
                signals.append("DEX_CONCENTRATION: Single DEX handling >80% of volume")
                
        return signals
    
    def _detect_whale_activity(self, analysis: Dict) -> Dict[str, Any]:
        """Detect whale trading activity"""
        whale_metrics = {
            "large_swaps_detected": analysis["swap_patterns"]["large_swaps"],
            "potential_whales": [],
            "whale_tokens": []
        }
        
        # Identify potential whales (high volume traders)
        for trader, data in list(analysis["trader_activity"].items())[:5]:
            if data["total_volume"] > 50:  # 50+ SOL volume
                whale_metrics["potential_whales"].append({
                    "trader": trader[:8] + "...",
                    "volume_sol": round(data["total_volume"], 2),
                    "swap_count": data["swap_count"],
                    "tokens_traded": data["tokens_traded"]
                })
                
        return whale_metrics
    
    def run_analysis(self) -> Dict[str, Any]:
        """Run the complete token swap analysis"""
        logger.info("Starting comprehensive token swap analysis...")
        
        # 1. Trading patterns analysis
        trading_analysis = self.analyze_trading_patterns(hours_back=24, sample_size=1000)
        
        # 2. Memecoin trends analysis
        memecoin_analysis = self.analyze_memecoin_trends(sample_size=500)
        
        # 3. Generate insights
        insights = self.generate_trading_insights(trading_analysis, memecoin_analysis)
        
        # Compile full results
        results = {
            "analysis_timestamp": datetime.now().isoformat(),
            "trading_patterns": trading_analysis,
            "memecoin_trends": memecoin_analysis,
            "insights": insights,
            "summary": {
                "total_files_analyzed": trading_analysis["files_analyzed"],
                "total_swaps": trading_analysis["swap_patterns"]["total_swaps"],
                "unique_tokens": len(trading_analysis["hot_tokens"]),
                "unique_traders": len(trading_analysis["trader_activity"]),
                "pump_fun_activity": memecoin_analysis["pump_fun_activity"]["total_swaps"]
            }
        }
        
        return results

def main():
    """Main function to run token swap analysis"""
    analyzer = TokenSwapAnalyzer()
    results = analyzer.run_analysis()
    
    # Print summary
    print("\n" + "="*80)
    print("TOKEN SWAP ANALYSIS SUMMARY")
    print("="*80)
    
    summary = results["summary"]
    print(f"Analysis completed at: {results['analysis_timestamp']}")
    print(f"Files analyzed: {summary['total_files_analyzed']}")
    print(f"Total swaps detected: {summary['total_swaps']}")
    print(f"Unique tokens: {summary['unique_tokens']}")
    print(f"Unique traders: {summary['unique_traders']}")
    print(f"Pump.fun activity: {summary['pump_fun_activity']} swaps")
    
    # Market overview
    market = results["insights"]["market_overview"]
    print(f"\nMarket Overview:")
    print(f"- SOL pair dominance: {market['sol_dominance']:.1%}")
    print(f"- Memecoin activity: {market['memecoin_share']:.1%}")
    print(f"- Large swap ratio: {market['large_swap_ratio']:.1%}")
    print(f"- Most active DEX: {market['most_active_dex'][0]} ({market['most_active_dex'][1]} swaps)")
    
    # Hot opportunities
    opportunities = results["insights"]["hot_opportunities"]
    if opportunities:
        print(f"\nHot Trading Opportunities:")
        for i, opp in enumerate(opportunities[:3], 1):
            print(f"{i}. {opp['token']} - {opp['activity_score']} swaps, {opp['unique_traders']} traders, avg {opp['avg_swap_size']} SOL")
    
    # Memecoin alerts
    alerts = results["insights"]["memecoin_alerts"]
    if alerts:
        print(f"\nMemecoin Alerts:")
        for alert in alerts[:3]:
            print(f"- {alert['token']}: {alert['swap_count']} swaps, {alert['trader_count']} traders ({alert['alert_level']})")
    
    # Risk signals
    risks = results["insights"]["risk_signals"]
    if risks:
        print(f"\nRisk Signals:")
        for risk in risks:
            print(f"- {risk}")
    
    # Whale activity
    whales = results["insights"]["whale_activity"]
    print(f"\nWhale Activity:")
    print(f"- Large swaps detected: {whales['large_swaps_detected']}")
    if whales["potential_whales"]:
        print(f"- Potential whales: {len(whales['potential_whales'])}")
        for whale in whales["potential_whales"][:2]:
            print(f"  • {whale['trader']}: {whale['volume_sol']} SOL volume, {whale['swap_count']} swaps")
    
    # Save detailed report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"/home/jgupdogg/dev/claude_pipeline/data/token_swap_analysis_{timestamp}.json"
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nDetailed analysis saved to: {output_file}")
    print("="*80)

if __name__ == "__main__":
    main()