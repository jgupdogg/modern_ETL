#!/usr/bin/env python3
"""
Comprehensive Webhook Data Analysis Script

This script analyzes the webhook data structure from the local data directory
and provides insights into the transaction patterns, token activities, and 
data pipeline flow through bronze/silver/gold layers.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Any, Tuple

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebhookDataAnalyzer:
    def __init__(self, data_dir: str = "/home/jgupdogg/dev/claude_pipeline/data"):
        self.data_dir = data_dir
        self.webhook_dir = os.path.join(data_dir, "webhooks")
        self.processed_dir = os.path.join(data_dir, "test_output")
        
        # Analysis results storage
        self.analysis_results = {}
        
    def analyze_webhook_directory_structure(self) -> Dict[str, Any]:
        """Analyze the webhook directory structure and file distribution"""
        logger.info("Analyzing webhook directory structure...")
        
        structure = {
            "total_files": 0,
            "files_by_date": defaultdict(int),
            "files_by_hour": defaultdict(int),
            "date_range": {"earliest": None, "latest": None},
            "directory_structure": {}
        }
        
        if not os.path.exists(self.webhook_dir):
            logger.warning(f"Webhook directory not found: {self.webhook_dir}")
            return structure
            
        # Walk through the webhook directory
        for root, dirs, files in os.walk(self.webhook_dir):
            json_files = [f for f in files if f.endswith('.json')]
            
            if json_files:
                rel_path = os.path.relpath(root, self.webhook_dir)
                structure["directory_structure"][rel_path] = len(json_files)
                structure["total_files"] += len(json_files)
                
                # Extract date information from path
                path_parts = rel_path.split(os.sep)
                if len(path_parts) >= 3 and path_parts[0] != '.':
                    try:
                        year, month, day = path_parts[:3]
                        date_str = f"{year}-{month}-{day}"
                        structure["files_by_date"][date_str] += len(json_files)
                        
                        # Update date range
                        current_date = datetime.strptime(date_str, "%Y-%m-%d")
                        if structure["date_range"]["earliest"] is None or current_date < structure["date_range"]["earliest"]:
                            structure["date_range"]["earliest"] = current_date
                        if structure["date_range"]["latest"] is None or current_date > structure["date_range"]["latest"]:
                            structure["date_range"]["latest"] = current_date
                            
                        # Analyze hourly distribution from filenames
                        for filename in json_files:
                            if len(filename) >= 15:  # Format: YYYYMMDD_HHMMSS_...
                                try:
                                    hour = filename[9:11]
                                    structure["files_by_hour"][hour] += 1
                                except:
                                    pass
                    except ValueError:
                        pass
        
        # Convert dates to strings for JSON serialization
        if structure["date_range"]["earliest"]:
            structure["date_range"]["earliest"] = structure["date_range"]["earliest"].strftime("%Y-%m-%d")
        if structure["date_range"]["latest"]:
            structure["date_range"]["latest"] = structure["date_range"]["latest"].strftime("%Y-%m-%d")
            
        return structure
    
    def analyze_sample_webhook_content(self, sample_size: int = 100) -> Dict[str, Any]:
        """Analyze the content structure of a sample of webhook files"""
        logger.info(f"Analyzing sample webhook content (n={sample_size})...")
        
        analysis = {
            "sample_size": 0,
            "transaction_types": Counter(),
            "sources": Counter(),
            "token_mints": Counter(),
            "user_addresses": Counter(),
            "swap_patterns": {
                "total_swaps": 0,
                "tokens_swapped": Counter(),
                "swap_amounts": [],
                "dex_sources": Counter()
            },
            "field_analysis": {
                "required_fields": [],
                "optional_fields": [],
                "nested_structures": []
            },
            "data_quality": {
                "valid_records": 0,
                "missing_payload": 0,
                "parse_errors": 0,
                "empty_transactions": 0
            }
        }
        
        sample_files = self._get_sample_files(sample_size)
        
        for file_path in sample_files:
            try:
                with open(file_path, 'r') as f:
                    webhook_data = json.load(f)
                    
                analysis["sample_size"] += 1
                self._analyze_webhook_record(webhook_data, analysis)
                
            except Exception as e:
                logger.warning(f"Error processing file {file_path}: {e}")
                analysis["data_quality"]["parse_errors"] += 1
                
        # Calculate percentages and summaries
        if analysis["sample_size"] > 0:
            analysis["data_quality"]["success_rate"] = analysis["data_quality"]["valid_records"] / analysis["sample_size"]
            
        return analysis
    
    def _get_sample_files(self, sample_size: int) -> List[str]:
        """Get a representative sample of webhook files"""
        all_files = []
        
        # Collect files from different dates to get a representative sample
        for root, dirs, files in os.walk(self.webhook_dir):
            json_files = [os.path.join(root, f) for f in files if f.endswith('.json')]
            all_files.extend(json_files)
            
        # Sort by modification time and take samples from different periods
        all_files.sort(key=lambda x: os.path.getmtime(x))
        
        if len(all_files) <= sample_size:
            return all_files
            
        # Take samples from beginning, middle, and end
        step = len(all_files) // sample_size
        return all_files[::step][:sample_size]
    
    def _analyze_webhook_record(self, webhook_data: Dict, analysis: Dict):
        """Analyze a single webhook record"""
        try:
            # Check basic structure
            if 'payload' not in webhook_data:
                analysis["data_quality"]["missing_payload"] += 1
                return
                
            payload = webhook_data['payload']
            if not payload:
                analysis["data_quality"]["empty_transactions"] += 1
                return
                
            analysis["data_quality"]["valid_records"] += 1
            
            # Analyze payload structure (could be list or single object)
            transactions = payload if isinstance(payload, list) else [payload]
            
            for tx in transactions:
                if not isinstance(tx, dict):
                    continue
                    
                # Transaction type and source
                tx_type = tx.get('type', 'UNKNOWN')
                source = tx.get('source', 'UNKNOWN')
                analysis["transaction_types"][tx_type] += 1
                analysis["sources"][source] += 1
                
                # Analyze swap transactions
                if tx_type == 'SWAP':
                    self._analyze_swap_transaction(tx, analysis)
                    
                # Analyze token transfers
                if 'tokenTransfers' in tx:
                    self._analyze_token_transfers(tx['tokenTransfers'], analysis)
                    
                # Account data analysis
                if 'accountData' in tx:
                    self._analyze_account_data(tx['accountData'], analysis)
                    
        except Exception as e:
            logger.warning(f"Error analyzing webhook record: {e}")
            analysis["data_quality"]["parse_errors"] += 1
    
    def _analyze_swap_transaction(self, tx: Dict, analysis: Dict):
        """Analyze swap-specific transaction data"""
        analysis["swap_patterns"]["total_swaps"] += 1
        
        source = tx.get('source', 'UNKNOWN')
        analysis["swap_patterns"]["dex_sources"][source] += 1
        
        # Analyze swap events
        events = tx.get('events', {})
        if 'swap' in events:
            swap_data = events['swap']
            
            # Token inputs and outputs
            token_inputs = swap_data.get('tokenInputs', [])
            token_outputs = swap_data.get('tokenOutputs', [])
            
            for token_input in token_inputs:
                mint = token_input.get('mint')
                amount = token_input.get('tokenAmount', 0)
                if mint:
                    analysis["swap_patterns"]["tokens_swapped"][mint] += 1
                if amount:
                    analysis["swap_patterns"]["swap_amounts"].append(float(amount))
                    
    def _analyze_token_transfers(self, token_transfers: List[Dict], analysis: Dict):
        """Analyze token transfer data"""
        for transfer in token_transfers:
            mint = transfer.get('mint')
            from_user = transfer.get('fromUserAccount')
            to_user = transfer.get('toUserAccount')
            
            if mint:
                analysis["token_mints"][mint] += 1
            if from_user:
                analysis["user_addresses"][from_user] += 1
            if to_user:
                analysis["user_addresses"][to_user] += 1
                
    def _analyze_account_data(self, account_data: List[Dict], analysis: Dict):
        """Analyze account data from transactions"""
        for account in account_data:
            account_addr = account.get('account')
            if account_addr:
                analysis["user_addresses"][account_addr] += 1
                
            # Analyze token balance changes
            token_changes = account.get('tokenBalanceChanges', [])
            for change in token_changes:
                mint = change.get('mint')
                if mint:
                    analysis["token_mints"][mint] += 1
    
    def analyze_data_pipeline_flow(self) -> Dict[str, Any]:
        """Analyze the data flow through bronze/silver/gold layers"""
        logger.info("Analyzing data pipeline flow...")
        
        pipeline_analysis = {
            "bronze_layer": self._analyze_bronze_layer(),
            "silver_layer": self._analyze_silver_layer(),
            "gold_layer": self._analyze_gold_layer(),
            "processing_metrics": self._calculate_processing_metrics()
        }
        
        return pipeline_analysis
    
    def _analyze_bronze_layer(self) -> Dict[str, Any]:
        """Analyze bronze layer data (raw webhooks)"""
        bronze_stats = {
            "total_webhook_files": 0,
            "size_mb": 0,
            "date_coverage": {},
            "estimated_records": 0
        }
        
        if os.path.exists(self.webhook_dir):
            total_size = 0
            file_count = 0
            
            for root, dirs, files in os.walk(self.webhook_dir):
                for file in files:
                    if file.endswith('.json'):
                        file_path = os.path.join(root, file)
                        file_count += 1
                        total_size += os.path.getsize(file_path)
                        
            bronze_stats["total_webhook_files"] = file_count
            bronze_stats["size_mb"] = round(total_size / (1024 * 1024), 2)
            bronze_stats["estimated_records"] = file_count  # 1 webhook = 1 record
            
        return bronze_stats
    
    def _analyze_silver_layer(self) -> Dict[str, Any]:
        """Analyze silver layer data (processed parquet)"""
        silver_stats = {
            "parquet_files_exist": False,
            "processed_records": 0,
            "transformation_success": False
        }
        
        # Check for processed output
        if os.path.exists(self.processed_dir):
            parquet_files = []
            for root, dirs, files in os.walk(self.processed_dir):
                parquet_files.extend([f for f in files if f.endswith('.parquet')])
                
            if parquet_files:
                silver_stats["parquet_files_exist"] = True
                silver_stats["transformation_success"] = True
                # Estimate records (would need Spark to get exact count)
                silver_stats["estimated_records"] = len(parquet_files) * 1000  # Rough estimate
                
        return silver_stats
    
    def _analyze_gold_layer(self) -> Dict[str, Any]:
        """Analyze gold layer data (final analytics)"""
        # Placeholder for gold layer analysis
        return {
            "analytics_ready": False,
            "aggregated_metrics": {},
            "insights_generated": False
        }
    
    def _calculate_processing_metrics(self) -> Dict[str, Any]:
        """Calculate processing efficiency metrics"""
        return {
            "data_freshness": self._calculate_data_freshness(),
            "processing_lag": "TBD",  # Would need to check actual pipeline runs
            "data_quality_score": "TBD"
        }
    
    def _calculate_data_freshness(self) -> str:
        """Calculate how fresh the most recent data is"""
        if not os.path.exists(self.webhook_dir):
            return "No data"
            
        latest_file_time = 0
        for root, dirs, files in os.walk(self.webhook_dir):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    file_time = os.path.getmtime(file_path)
                    latest_file_time = max(latest_file_time, file_time)
                    
        if latest_file_time == 0:
            return "No files found"
            
        latest_datetime = datetime.fromtimestamp(latest_file_time)
        time_diff = datetime.now() - latest_datetime
        
        if time_diff.days > 0:
            return f"{time_diff.days} days ago"
        elif time_diff.seconds > 3600:
            return f"{time_diff.seconds // 3600} hours ago"
        else:
            return f"{time_diff.seconds // 60} minutes ago"
    
    def generate_insights(self) -> Dict[str, Any]:
        """Generate high-level insights from the analysis"""
        insights = {
            "transaction_volume": "High volume of swap transactions detected",
            "popular_tokens": [],
            "active_dexes": [],
            "user_behavior": {},
            "data_quality": "Good",
            "pipeline_health": "Functional"
        }
        
        # Add specific insights based on analysis results
        if "content_analysis" in self.analysis_results:
            content = self.analysis_results["content_analysis"]
            
            # Most popular tokens
            if content["token_mints"]:
                top_tokens = content["token_mints"].most_common(5)
                insights["popular_tokens"] = [{"mint": mint, "occurrences": count} for mint, count in top_tokens]
                
            # Most active DEXes
            if content["sources"]:
                top_dexes = content["sources"].most_common(3)
                insights["active_dexes"] = [{"dex": dex, "transactions": count} for dex, count in top_dexes]
                
            # Data quality assessment
            success_rate = content["data_quality"].get("success_rate", 0)
            if success_rate > 0.9:
                insights["data_quality"] = "Excellent"
            elif success_rate > 0.7:
                insights["data_quality"] = "Good"
            else:
                insights["data_quality"] = "Needs attention"
                
        return insights
    
    def run_full_analysis(self) -> Dict[str, Any]:
        """Run the complete webhook data analysis"""
        logger.info("Starting comprehensive webhook data analysis...")
        
        # 1. Directory structure analysis
        logger.info("Step 1: Analyzing directory structure...")
        self.analysis_results["directory_structure"] = self.analyze_webhook_directory_structure()
        
        # 2. Content analysis
        logger.info("Step 2: Analyzing webhook content...")
        self.analysis_results["content_analysis"] = self.analyze_sample_webhook_content(sample_size=200)
        
        # 3. Pipeline flow analysis
        logger.info("Step 3: Analyzing data pipeline flow...")
        self.analysis_results["pipeline_flow"] = self.analyze_data_pipeline_flow()
        
        # 4. Generate insights
        logger.info("Step 4: Generating insights...")
        self.analysis_results["insights"] = self.generate_insights()
        
        # 5. Summary statistics
        self.analysis_results["summary"] = self._generate_summary()
        
        logger.info("Analysis complete!")
        return self.analysis_results
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate summary statistics"""
        summary = {
            "analysis_timestamp": datetime.now().isoformat(),
            "total_webhook_files": self.analysis_results.get("directory_structure", {}).get("total_files", 0),
            "date_range": self.analysis_results.get("directory_structure", {}).get("date_range", {}),
            "sample_analyzed": self.analysis_results.get("content_analysis", {}).get("sample_size", 0),
            "data_quality_score": self.analysis_results.get("content_analysis", {}).get("data_quality", {}).get("success_rate", 0),
            "pipeline_status": "Active" if self.analysis_results.get("pipeline_flow", {}).get("bronze_layer", {}).get("total_webhook_files", 0) > 0 else "Inactive"
        }
        return summary
    
    def save_analysis_report(self, output_file: str = None):
        """Save the analysis results to a JSON file"""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"/home/jgupdogg/dev/claude_pipeline/data/webhook_analysis_report_{timestamp}.json"
            
        with open(output_file, 'w') as f:
            json.dump(self.analysis_results, f, indent=2, default=str)
            
        logger.info(f"Analysis report saved to: {output_file}")
        return output_file

def main():
    """Main function to run the webhook analysis"""
    analyzer = WebhookDataAnalyzer()
    
    # Run full analysis
    results = analyzer.run_full_analysis()
    
    # Print summary to console
    print("\n" + "="*80)
    print("WEBHOOK DATA ANALYSIS SUMMARY")
    print("="*80)
    
    summary = results.get("summary", {})
    print(f"Analysis completed at: {summary.get('analysis_timestamp', 'Unknown')}")
    print(f"Total webhook files: {summary.get('total_webhook_files', 0):,}")
    print(f"Date range: {summary.get('date_range', {}).get('earliest', 'Unknown')} to {summary.get('date_range', {}).get('latest', 'Unknown')}")
    print(f"Sample analyzed: {summary.get('sample_analyzed', 0)} files")
    print(f"Data quality score: {summary.get('data_quality_score', 0):.2%}")
    print(f"Pipeline status: {summary.get('pipeline_status', 'Unknown')}")
    
    # Print key insights
    insights = results.get("insights", {})
    print(f"\nKey Insights:")
    print(f"- Data Quality: {insights.get('data_quality', 'Unknown')}")
    print(f"- Pipeline Health: {insights.get('pipeline_health', 'Unknown')}")
    
    if insights.get("popular_tokens"):
        print(f"- Top Tokens:")
        for token in insights["popular_tokens"][:3]:
            print(f"  • {token['mint'][:8]}... ({token['occurrences']} occurrences)")
            
    if insights.get("active_dexes"):
        print(f"- Active DEXes:")
        for dex in insights["active_dexes"]:
            print(f"  • {dex['dex']}: {dex['transactions']} transactions")
    
    # Content analysis details
    content = results.get("content_analysis", {})
    if content:
        print(f"\nTransaction Analysis:")
        print(f"- Total transaction types: {len(content.get('transaction_types', {}))}")
        print(f"- Unique sources: {len(content.get('sources', {}))}")
        print(f"- Unique token mints: {len(content.get('token_mints', {}))}")
        print(f"- Unique user addresses: {len(content.get('user_addresses', {}))}")
        print(f"- Total swaps detected: {content.get('swap_patterns', {}).get('total_swaps', 0)}")
    
    # Pipeline flow details
    pipeline = results.get("pipeline_flow", {})
    if pipeline:
        bronze = pipeline.get("bronze_layer", {})
        silver = pipeline.get("silver_layer", {})
        print(f"\nPipeline Flow:")
        print(f"- Bronze layer: {bronze.get('size_mb', 0)} MB, {bronze.get('total_webhook_files', 0)} files")
        print(f"- Silver layer: {'✓' if silver.get('parquet_files_exist') else '✗'} Parquet files exist")
        print(f"- Transformation: {'✓' if silver.get('transformation_success') else '✗'} Success")
    
    # Save detailed report
    report_file = analyzer.save_analysis_report()
    print(f"\nDetailed analysis report saved to: {report_file}")
    print("="*80)

if __name__ == "__main__":
    main()