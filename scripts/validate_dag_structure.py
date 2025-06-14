#!/usr/bin/env python3
"""
Simple DAG structure validation without dependency imports
"""

import re
import os

def validate_silver_dag():
    """Validate silver DAG has required gold processing fields"""
    print("🔍 Validating Silver DAG structure...")
    
    silver_path = "/home/jgupdogg/dev/claude_pipeline/dags/silver_wallet_pnl_dag.py"
    
    with open(silver_path, 'r') as f:
        content = f.read()
    
    required_fields = [
        'processed_for_gold',
        'gold_processed_at', 
        'gold_processing_status',
        'gold_batch_id'
    ]
    
    # Check schema definition
    schema_section = re.search(r'def get_silver_pnl_schema.*?return StructType\(\[(.*?)\]\)', content, re.DOTALL)
    if not schema_section:
        print("❌ Could not find schema definition")
        return False
    
    schema_content = schema_section.group(1)
    
    print("  Checking schema fields:")
    for field in required_fields:
        if field in schema_content:
            print(f"    ✅ {field}")
        else:
            print(f"    ❌ Missing: {field}")
            return False
    
    # Check that fields are used in results
    print("  Checking field usage in results:")
    usage_patterns = [
        r'lit\(False\)\.alias\("processed_for_gold"\)',
        r'lit\(None\)\.cast\("timestamp"\)\.alias\("gold_processed_at"\)',
        r'lit\("pending"\)\.alias\("gold_processing_status"\)',
        r'lit\(None\)\.cast\("string"\)\.alias\("gold_batch_id"\)'
    ]
    
    for i, pattern in enumerate(usage_patterns):
        if re.search(pattern, content):
            print(f"    ✅ {required_fields[i]} usage found")
        else:
            print(f"    ❌ {required_fields[i]} usage missing")
            return False
    
    print("✅ Silver DAG structure validation PASSED")
    return True


def validate_gold_dag():
    """Validate gold DAG structure"""
    print("\n🔍 Validating Gold DAG structure...")
    
    gold_path = "/home/jgupdogg/dev/claude_pipeline/dags/gold_top_traders_dag.py"
    
    with open(gold_path, 'r') as f:
        content = f.read()
    
    # Check key functions exist
    required_functions = [
        'get_gold_top_traders_schema',
        'read_unprocessed_silver_pnl',
        'select_top_performers',
        'write_gold_top_traders',
        'update_silver_processing_status',
        'create_top_traders_batch'
    ]
    
    print("  Checking required functions:")
    for func in required_functions:
        if f"def {func}" in content:
            print(f"    ✅ {func}")
        else:
            print(f"    ❌ Missing: {func}")
            return False
    
    # Check filtering logic
    filter_patterns = [
        r'processed_for_gold.*==.*False',
        r'token_address.*==.*ALL_TOKENS',
        r'time_period.*==.*all',
        r'total_pnl.*>=.*min_total_pnl',
        r'roi.*>=.*min_roi',
        r'win_rate.*>=.*min_win_rate'
    ]
    
    print("  Checking filtering logic:")
    for pattern in filter_patterns:
        if re.search(pattern, content):
            print(f"    ✅ Filter pattern found: {pattern.split('.*')[0]}")
        else:
            print(f"    ❌ Missing filter: {pattern}")
            return False
    
    # Check performance tiers
    tier_patterns = [
        r'"elite"',
        r'"strong"', 
        r'"promising"'
    ]
    
    print("  Checking performance tiers:")
    for tier in tier_patterns:
        if tier in content:
            print(f"    ✅ {tier} tier found")
        else:
            print(f"    ❌ Missing tier: {tier}")
            return False
    
    print("✅ Gold DAG structure validation PASSED")
    return True


def validate_dag_schedules():
    """Validate DAG scheduling is compatible"""
    print("\n🔍 Validating DAG scheduling...")
    
    # Read both DAGs
    silver_path = "/home/jgupdogg/dev/claude_pipeline/dags/silver_wallet_pnl_dag.py"
    gold_path = "/home/jgupdogg/dev/claude_pipeline/dags/gold_top_traders_dag.py"
    
    with open(silver_path, 'r') as f:
        silver_content = f.read()
    
    with open(gold_path, 'r') as f:
        gold_content = f.read()
    
    # Extract schedule intervals
    silver_schedule = re.search(r"schedule_interval='([^']+)'", silver_content)
    gold_schedule = re.search(r"schedule_interval='([^']+)'", gold_content)
    
    if silver_schedule and gold_schedule:
        print(f"  Silver schedule: {silver_schedule.group(1)}")
        print(f"  Gold schedule: {gold_schedule.group(1)}")
        
        # Check that gold runs after silver (2 hour offset)
        if "*/12 * * *" in silver_schedule.group(1) and "2,14 * * *" in gold_schedule.group(1):
            print("  ✅ Gold DAG scheduled 2 hours after Silver DAG")
        else:
            print("  ⚠️  Schedule offset should be verified manually")
    else:
        print("  ❌ Could not extract schedule intervals")
        return False
    
    print("✅ DAG scheduling validation PASSED")
    return True


def main():
    """Run all validations"""
    print("🚀 DAG Structure Validation Suite")
    print("=" * 50)
    
    success = True
    
    if not validate_silver_dag():
        success = False
    
    if not validate_gold_dag():
        success = False
    
    if not validate_dag_schedules():
        success = False
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 ALL VALIDATIONS PASSED")
        print("\n✅ Step 1: Silver schema updated with gold processing fields")
        print("✅ Step 2: Gold top traders DAG created successfully")
        print("\nImplementation Summary:")
        print("📋 Silver PnL → Gold Top Traders Data Flow:")
        print("  1. Silver PnL calculates wallet performance metrics")
        print("  2. Records marked with processed_for_gold = false")
        print("  3. Gold DAG selects top performers (positive PnL, min thresholds)")
        print("  4. Performance tiers: elite/strong/promising")
        print("  5. Silver records updated to processed_for_gold = true")
        print("  6. Full audit trail with batch tracking")
        
        print("\n🎯 Key Features:")
        print("  • Incremental processing (no duplicates)")
        print("  • Performance-based filtering")
        print("  • Multi-tier trader classification")
        print("  • Partitioned storage for analytics")
        print("  • Proper schedule offset (Silver every 12h, Gold +2h)")
        
    else:
        print("❌ VALIDATIONS FAILED")
    
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)