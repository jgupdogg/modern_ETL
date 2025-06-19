#!/usr/bin/env python3
"""
Airflow DAG: DBT Smart Wallets Transformation
Filters high-frequency trading bots and selects profitable smart wallets from silver layer.
"""

import os
import sys
import subprocess
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# DAG Configuration
DAG_ID = "dbt_smart_wallets_transformation"
SCHEDULE_INTERVAL = "@daily"  # Run daily after smart trader identification pipeline

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DBT smart wallets transformation - bot filtering and profitable trader selection',
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'smart-wallets', 'gold', 'bot-filtering', 'medallion-architecture']
)

def run_dbt_command(command):
    """Execute DBT command with proper configuration."""
    env = os.environ.copy()
    env['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
    
    result = subprocess.run(
        command.split(),
        cwd='/opt/airflow/dbt',
        env=env,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"DBT command failed: {result.stderr}")
    
    print(result.stdout)
    return result.stdout

def cleanup_dbt_artifacts():
    """Clean up dbt target directory to prevent accumulation."""
    import shutil
    from pathlib import Path
    
    dbt_target_dir = Path('/opt/airflow/dbt/target')
    
    if not dbt_target_dir.exists():
        print("dbt target directory does not exist, skipping cleanup")
        return {"status": "skipped", "reason": "directory_not_found"}
    
    files_deleted = 0
    space_freed = 0
    
    print("🧹 Cleaning dbt artifacts...")
    
    # Clean up old compilation artifacts (keep only current)
    files_to_clean = [
        'run_results.json.bak',
        'manifest.json.old',
        'partial_parse.msgpack.old',
        'graph.gpickle.old'
    ]
    
    for file_pattern in files_to_clean:
        for file_path in dbt_target_dir.glob(file_pattern):
            try:
                file_size = file_path.stat().st_size
                file_path.unlink()
                files_deleted += 1
                space_freed += file_size
                print(f"Deleted: {file_path}")
            except (OSError, FileNotFoundError):
                continue
    
    # Clean up old run/test directories (keep only latest)
    for subdir in ['run', 'test']:
        subdir_path = dbt_target_dir / subdir
        if subdir_path.exists():
            try:
                # Remove files older than 1 day
                cutoff_time = (datetime.now() - timedelta(days=1)).timestamp()
                for item in subdir_path.rglob('*'):
                    if item.is_file() and item.stat().st_mtime < cutoff_time:
                        try:
                            file_size = item.stat().st_size
                            item.unlink()
                            files_deleted += 1
                            space_freed += file_size
                        except (OSError, FileNotFoundError):
                            continue
            except Exception as e:
                print(f"Warning: Could not clean {subdir}: {e}")
    
    space_freed_mb = space_freed / 1024 / 1024
    result = {
        "status": "success",
        "files_deleted": files_deleted,
        "space_freed_mb": round(space_freed_mb, 2)
    }
    
    print(f"✅ dbt cleanup completed: {files_deleted} files, {space_freed_mb:.2f}MB freed")
    return result

# Pre-cleanup task
dbt_cleanup_artifacts = PythonOperator(
    task_id='dbt_cleanup_artifacts',
    python_callable=cleanup_dbt_artifacts,
    dag=dag
)

# DBT Commands for smart wallets model
dbt_run_smart_wallets = PythonOperator(
    task_id='dbt_run_smart_wallets',
    python_callable=run_dbt_command,
    op_args=['dbt run --models smart_wallets'],
    dag=dag,
    retries=2
)

dbt_test_smart_wallets = PythonOperator(
    task_id='dbt_test_smart_wallets',
    python_callable=run_dbt_command,
    op_args=['dbt test --models smart_wallets'],
    dag=dag
)

@task(dag=dag)
def validate_smart_wallets_output(**context) -> Dict[str, Any]:
    """Validate smart wallets output and bot filtering effectiveness."""
    import subprocess
    import json
    
    # Run validation query via DuckDB
    validation_cmd = """
    docker exec claude_pipeline-duckdb python3 -c "
import duckdb
conn = duckdb.connect('/data/analytics.duckdb')
conn.execute('LOAD httpfs;')
conn.execute('SET s3_endpoint=\\'minio:9000\\';')
conn.execute('SET s3_access_key_id=\\'minioadmin\\';')
conn.execute('SET s3_secret_access_key=\\'minioadmin123\\';')
conn.execute('SET s3_use_ssl=false;')
conn.execute('SET s3_url_style=\\'path\\';')

try:
    # Smart wallets counts
    smart_wallets_count = conn.execute('SELECT COUNT(*) FROM smart_wallets;').fetchone()[0]
    unique_smart_wallets = conn.execute('SELECT COUNT(DISTINCT wallet_address) FROM smart_wallets;').fetchone()[0]
    
    # Classification distribution
    elite_traders = conn.execute('SELECT COUNT(*) FROM smart_wallets WHERE trader_classification = \\\"elite_trader\\\";').fetchone()[0]
    profitable_traders = conn.execute('SELECT COUNT(*) FROM smart_wallets WHERE trader_classification = \\\"profitable_trader\\\";').fetchone()[0]
    consistent_traders = conn.execute('SELECT COUNT(*) FROM smart_wallets WHERE trader_classification = \\\"consistent_trader\\\";').fetchone()[0]
    emerging_traders = conn.execute('SELECT COUNT(*) FROM smart_wallets WHERE trader_classification = \\\"emerging_trader\\\";').fetchone()[0]
    
    # Bot filtering validation
    max_trade_frequency = conn.execute('SELECT MAX(trade_frequency_daily) FROM smart_wallets;').fetchone()[0]
    min_positive_pnl = conn.execute('SELECT MIN(GREATEST(realized_pnl, unrealized_pnl, total_pnl, all_time_total_pnl, roi, all_time_roi_pct)) FROM smart_wallets;').fetchone()[0]
    
    # Performance metrics
    avg_smart_score = conn.execute('SELECT AVG(smart_trader_score) FROM smart_wallets;').fetchone()[0]
    avg_all_time_pnl = conn.execute('SELECT AVG(all_time_total_pnl) FROM smart_wallets WHERE all_time_total_pnl > 0;').fetchone()[0]
    avg_win_rate = conn.execute('SELECT AVG(win_rate) FROM smart_wallets WHERE win_rate > 0;').fetchone()[0]
    
    # Top performer
    top_trader = conn.execute('SELECT wallet_address, all_time_total_pnl, trader_classification FROM smart_wallets ORDER BY all_time_total_pnl DESC LIMIT 1;').fetchone()
    
    # Compare with original silver data
    original_silver_count = conn.execute('SELECT COUNT(DISTINCT wallet_address) FROM read_parquet(\\\"s3://solana-data/silver/wallet_pnl/**/*.parquet\\\");').fetchone()[0]
    
    print(f'VALIDATION_RESULT:{{\\\"smart_wallets_total\\\":{smart_wallets_count},\\\"unique_smart_wallets\\\":{unique_smart_wallets},\\\"elite_traders\\\":{elite_traders},\\\"profitable_traders\\\":{profitable_traders},\\\"consistent_traders\\\":{consistent_traders},\\\"emerging_traders\\\":{emerging_traders},\\\"max_trade_frequency\\\":{max_trade_frequency or 0},\\\"min_positive_pnl\\\":{min_positive_pnl or 0},\\\"avg_smart_score\\\":{avg_smart_score or 0},\\\"avg_all_time_pnl\\\":{avg_all_time_pnl or 0},\\\"avg_win_rate\\\":{avg_win_rate or 0},\\\"top_trader_wallet\\\":\\\"{top_trader[0] if top_trader else \\\"N/A\\\"}\\\",\\\"top_trader_pnl\\\":{top_trader[1] if top_trader else 0},\\\"top_trader_class\\\":\\\"{top_trader[2] if top_trader else \\\"N/A\\\"}\\\",\\\"original_wallets\\\":{original_silver_count},\\\"filter_efficiency\\\":{(original_silver_count - unique_smart_wallets) / original_silver_count * 100 if original_silver_count > 0 else 0}}}')
except Exception as e:
    print(f'VALIDATION_ERROR:{e}')
conn.close()
"
    """
    
    result = subprocess.run(validation_cmd, shell=True, capture_output=True, text=True)
    
    # Parse validation results
    if "VALIDATION_RESULT:" in result.stdout:
        result_json = result.stdout.split("VALIDATION_RESULT:")[1].strip()
        validation_data = json.loads(result_json)
        
        # Calculate bot filtering effectiveness
        filter_efficiency = validation_data.get('filter_efficiency', 0)
        
        return {
            'status': 'success',
            'smart_wallets_total': validation_data['smart_wallets_total'],
            'unique_smart_wallets': validation_data['unique_smart_wallets'],
            'classification_distribution': {
                'elite_traders': validation_data['elite_traders'],
                'profitable_traders': validation_data['profitable_traders'],
                'consistent_traders': validation_data['consistent_traders'],
                'emerging_traders': validation_data['emerging_traders']
            },
            'bot_filtering_validation': {
                'max_trade_frequency': validation_data['max_trade_frequency'],
                'min_positive_pnl': validation_data['min_positive_pnl'],
                'bot_filter_effective': validation_data['max_trade_frequency'] < 100
            },
            'performance_metrics': {
                'avg_smart_score': validation_data['avg_smart_score'],
                'avg_all_time_pnl': validation_data['avg_all_time_pnl'],
                'avg_win_rate': validation_data['avg_win_rate']
            },
            'top_performer': {
                'wallet_address': validation_data['top_trader_wallet'],
                'all_time_pnl': validation_data['top_trader_pnl'],
                'classification': validation_data['top_trader_class']
            },
            'filtering_effectiveness': {
                'original_wallets': validation_data['original_wallets'],
                'smart_wallets': validation_data['unique_smart_wallets'],
                'filtered_out': validation_data['original_wallets'] - validation_data['unique_smart_wallets'],
                'filter_efficiency_pct': round(filter_efficiency, 2)
            },
            'validation_timestamp': context['ts'],
            'ready_for_helius': validation_data['max_trade_frequency'] < 100 and validation_data['min_positive_pnl'] > 0
        }
    else:
        raise ValueError(f"Smart wallets validation failed: {result.stdout}")

@task(dag=dag)
def generate_bot_filtering_summary(validation_result: Dict[str, Any], **context) -> Dict[str, Any]:
    """Generate summary report of bot filtering effectiveness."""
    
    summary = {
        'pipeline_run_date': context['ds'],
        'total_smart_wallets_identified': validation_result['unique_smart_wallets'],
        'bot_filtering_effectiveness': validation_result['filtering_effectiveness'],
        'trader_quality_distribution': validation_result['classification_distribution'],
        'top_performing_trader': validation_result['top_performer'],
        'average_performance_metrics': validation_result['performance_metrics'],
        'helius_integration_ready': validation_result['ready_for_helius'],
        'recommendations': []
    }
    
    # Add recommendations based on results
    filter_efficiency = validation_result['filtering_effectiveness']['filter_efficiency_pct']
    
    if filter_efficiency > 50:
        summary['recommendations'].append("High bot filtering efficiency - consider tightening thresholds")
    elif filter_efficiency < 20:
        summary['recommendations'].append("Low filtering efficiency - review bot detection criteria")
    
    if validation_result['classification_distribution']['elite_traders'] < 5:
        summary['recommendations'].append("Few elite traders identified - consider lowering trade frequency threshold")
    
    if validation_result['performance_metrics']['avg_win_rate'] < 50:
        summary['recommendations'].append("Low average win rate - strengthen profitability filters")
    
    print(f"🎯 BOT FILTERING SUMMARY:")
    print(f"   Smart Wallets Identified: {summary['total_smart_wallets_identified']}")
    print(f"   Bots/Poor Traders Filtered: {validation_result['filtering_effectiveness']['filtered_out']}")
    print(f"   Filtering Efficiency: {filter_efficiency}%")
    print(f"   Elite Traders: {validation_result['classification_distribution']['elite_traders']}")
    print(f"   Ready for Helius: {summary['helius_integration_ready']}")
    
    return summary

# Define task dependencies
with dag:
    # Sequential smart wallets transformation flow
    smart_wallets_validation = validate_smart_wallets_output()
    filtering_summary = generate_bot_filtering_summary(smart_wallets_validation)
    
    dbt_cleanup_artifacts >> dbt_run_smart_wallets >> dbt_test_smart_wallets >> smart_wallets_validation >> filtering_summary