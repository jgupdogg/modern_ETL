#!/usr/bin/env python3
"""
Organize Claude Pipeline Codebase
Moves test, migration, and temporary files to an organized archive structure
"""
import os
import shutil
from datetime import datetime
from pathlib import Path

# Define archive structure
ARCHIVE_BASE = Path("/home/jgupdogg/dev/claude_pipeline/archive")
TIMESTAMP = datetime.now().strftime("%Y%m%d")

# Archive categories and their target directories
ARCHIVE_STRUCTURE = {
    "delta_migration": f"delta_migration_{TIMESTAMP}",
    "test_scripts": f"test_scripts_{TIMESTAMP}",
    "migration_scripts": f"migration_scripts_{TIMESTAMP}",
    "documentation": f"delta_docs_{TIMESTAMP}",
    "debug_scripts": f"debug_scripts_{TIMESTAMP}",
    "temp_cleanup": f"temp_cleanup_{TIMESTAMP}"
}

# Files to archive by category
FILES_TO_ARCHIVE = {
    "test_scripts": {
        "dags": [
            "test_real_data_delta.py",
            "test_spark_delta_config.py"
        ],
        "scripts": [
            "test_delta_acid_properties.py",
            "test_delta_inside_docker.py",
            "test_delta_lake_health.py",
            "test_delta_only.py",
            "test_delta_operations.py",
            "test_delta_silver_tasks.py",
            "test_delta_write.py",
            "test_duckdb_pnl.py",
            "test_enhanced_smart_wallets.py",
            "test_gold_layer.py",
            "test_gold_logic.py",
            "test_gold_simple.py",
            "test_real_data_delta.py",
            "test_silver_pnl_delta.py",
            "test_spark_delta_config.py",
            "test_table_creation.py",
            "test_updated_gold_criteria.py"
        ]
    },
    "migration_scripts": {
        "scripts": [
            "migrate_bronze_token_list.py",
            "migrate_bronze_token_whales.py",
            "migrate_token_metrics.py",
            "migrate_whale_holders.py",
            "migrate_wallet_transactions.py",
            "simple_migration_check.py",
            "validate_migration.py",
            "validate_migration_quality.py",
            "convert_to_delta_lake.py",
            "delta_conversion_docker.py"
        ]
    },
    "documentation": {
        "root": [
            "DELTA_IMPLEMENTATION_PLAN.md",
            "DELTA_LAKE_MIGRATION_PLAN.md",
            "DELTA_LAKE_PHASE1_COMPLETE.md",
            "DELTA_MIGRATION_PLAN.md",
            "SMART_TRADER_COMPLETE_MIGRATION_PLAN.md",
            "WALLET_TRANSACTIONS_MIGRATION_PLAN.md",
            "PHASE3_PYSPARK_ELIMINATION_COMPLETE.md",
            "MIGRATION_IMPLEMENTATION_READY.md",
            "DAG_OPTIMIZATION_REVIEW.md"
        ]
    },
    "debug_scripts": {
        "scripts": [
            "debug_pnl_issue.py",
            "debug_filtering.py",
            "debug_transaction_processing.py",
            "inspect_transaction_schema.py",
            "investigate_tx_values.py"
        ]
    },
    "temp_cleanup": {
        "dags": [
            "cleanup_silver_pnl_dag.py",
            "clear_processing_logs_dag.py"
        ],
        "scripts": [
            "cleanup_silver_pnl.py",
            "remove_all_mock_data.py",
            "archive_mock_dags.sh",
            "safe_migration_check.sh",
            "cleanup_old_silver_data.py"
        ]
    },
    "old_implementations": {
        "tasks": [
            "dags/tasks/smart_traders/silver_tasks_old_collect.py"
        ]
    }
}

# Directories to move entirely
DIRECTORIES_TO_ARCHIVE = {
    "migration_dirs": [
        "scripts/migration",
        "scripts/migrations",
        "scripts/delta_migration"
    ],
    "test_dirs": [
        "scripts/tests",
        "scripts/validators"
    ]
}

def create_archive_structure():
    """Create the archive directory structure"""
    for category, dirname in ARCHIVE_STRUCTURE.items():
        archive_path = ARCHIVE_BASE / dirname
        archive_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created archive directory: {archive_path}")

def archive_files(dry_run=True):
    """Archive files to their respective directories"""
    archived_count = 0
    
    for category, locations in FILES_TO_ARCHIVE.items():
        target_dir = ARCHIVE_BASE / ARCHIVE_STRUCTURE.get(category, category)
        
        for location, files in locations.items():
            for filename in files:
                if location == "root":
                    source = Path(f"/home/jgupdogg/dev/claude_pipeline/{filename}")
                elif location == "tasks":
                    source = Path(f"/home/jgupdogg/dev/claude_pipeline/{filename}")
                else:
                    source = Path(f"/home/jgupdogg/dev/claude_pipeline/{location}/{filename}")
                
                if source.exists():
                    # Preserve directory structure in archive
                    if location not in ["root"]:
                        dest_dir = target_dir / location
                        dest_dir.mkdir(parents=True, exist_ok=True)
                        dest = dest_dir / filename
                    else:
                        dest = target_dir / filename
                    
                    if dry_run:
                        print(f"  Would move: {source} → {dest}")
                    else:
                        shutil.move(str(source), str(dest))
                        print(f"  ✓ Moved: {source} → {dest}")
                    archived_count += 1
                else:
                    print(f"  ⚠ Not found: {source}")
    
    return archived_count

def archive_directories(dry_run=True):
    """Archive entire directories"""
    archived_count = 0
    
    for category, dirs in DIRECTORIES_TO_ARCHIVE.items():
        for dir_path in dirs:
            source = Path(f"/home/jgupdogg/dev/claude_pipeline/{dir_path}")
            
            if source.exists():
                # Determine target based on directory type
                if "migration" in dir_path:
                    target_base = ARCHIVE_BASE / ARCHIVE_STRUCTURE["migration_scripts"]
                elif "test" in dir_path or "validator" in dir_path:
                    target_base = ARCHIVE_BASE / ARCHIVE_STRUCTURE["test_scripts"]
                else:
                    target_base = ARCHIVE_BASE / category
                
                dest = target_base / source.name
                
                if dry_run:
                    print(f"  Would move directory: {source} → {dest}")
                else:
                    shutil.move(str(source), str(dest))
                    print(f"  ✓ Moved directory: {source} → {dest}")
                archived_count += 1
            else:
                print(f"  ⚠ Directory not found: {source}")
    
    return archived_count

def create_archive_readme():
    """Create a README in the archive directory explaining the contents"""
    readme_content = f"""# Claude Pipeline Archive - {TIMESTAMP}

This archive contains test files, migration scripts, and documentation from the Delta Lake migration completed in June 2025.

## Archive Structure

### delta_migration_{TIMESTAMP}/
Contains all Delta Lake migration scripts and tools used during the transition from Parquet to Delta Lake storage.

### test_scripts_{TIMESTAMP}/
Test scripts used during development and migration. These validated the Delta Lake implementation and can be referenced for debugging.

### migration_scripts_{TIMESTAMP}/
Data migration utilities used to convert existing data to Delta Lake format.

### delta_docs_{TIMESTAMP}/
Documentation and planning files from the Delta Lake migration project.

### debug_scripts_{TIMESTAMP}/
Debugging scripts used to resolve issues during development.

### temp_cleanup_{TIMESTAMP}/
Temporary cleanup scripts and one-time use utilities.

## Why These Files Were Archived

1. **Delta Lake Migration Complete**: The migration to Delta Lake is complete and in production. Migration scripts are no longer needed for daily operations.

2. **Test Scripts**: Extensive testing was done during migration. These scripts are archived for reference but not needed for production.

3. **Documentation**: Migration planning documents are archived as they describe completed work.

4. **Cleanup**: The codebase was cleaned to improve clarity and reduce confusion for future development.

## Production Status

The active production DAG is: `optimized_delta_smart_trader_identification` using Delta Lake with ACID compliance.

## Restoration

If any of these files are needed, they can be restored from this archive directory.
"""
    
    readme_path = ARCHIVE_BASE / f"README_{TIMESTAMP}.md"
    with open(readme_path, 'w') as f:
        f.write(readme_content)
    print(f"\n✓ Created archive README: {readme_path}")

def main():
    """Main execution function"""
    print("🧹 Claude Pipeline Codebase Organization Tool")
    print("=" * 60)
    
    # First, do a dry run
    print("\n📋 DRY RUN - Showing what would be moved:")
    print("-" * 60)
    
    create_archive_structure()
    
    print("\n📁 Files to archive:")
    file_count = archive_files(dry_run=True)
    
    print("\n📂 Directories to archive:")
    dir_count = archive_directories(dry_run=True)
    
    print(f"\n📊 Summary: {file_count} files and {dir_count} directories would be archived")
    
    # Auto-proceed with archiving
    print("\n✅ Auto-confirming archive process...")
    response = 'yes'
    
    if response == 'yes':
        print("\n🚀 Starting archive process...")
        print("-" * 60)
        
        # Perform actual archiving
        file_count = archive_files(dry_run=False)
        dir_count = archive_directories(dry_run=False)
        
        # Create README
        create_archive_readme()
        
        print(f"\n✅ Archive complete! {file_count} files and {dir_count} directories archived")
        print(f"📍 Archive location: {ARCHIVE_BASE}")
    else:
        print("\n❌ Archive cancelled")

if __name__ == "__main__":
    main()