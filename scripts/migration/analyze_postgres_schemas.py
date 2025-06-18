#!/usr/bin/env python3
"""
Analyze PostgreSQL schemas for migration to MinIO bronze layer
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime

# Database configurations
DATABASES = {
    'pipeline_db': {
        'host': 'localhost',
        'port': 5433,
        'database': 'pipeline_db',
        'user': 'postgres',
        'password': 'postgres'
    },
    'solana': {
        'host': 'localhost',
        'port': 5433,
        'database': 'solana',
        'user': 'postgres',
        'password': 'postgres'
    },
    'solana_pipeline': {
        'host': 'localhost',
        'port': 5433,
        'database': 'solana_pipeline',
        'user': 'postgres',
        'password': 'postgres'
    },
    'solana_traders': {
        'host': 'localhost',
        'port': 5433,
        'database': 'solana_traders',
        'user': 'postgres',
        'password': 'postgres'
    },
    'solana_trending': {
        'host': 'localhost',
        'port': 5433,
        'database': 'solana_trending',
        'user': 'postgres',
        'password': 'postgres'
    }
}

# Tables to analyze
TABLES_TO_ANALYZE = [
    ('pipeline_db', 'public', 'trades_seek_by_time_raw'),
    ('pipeline_db', 'public', 'tracked_addresses_swaps'),
    ('pipeline_db', 'public', 'trending_whale_trades'),
    ('solana', 'public', 'wallet_trade_history'),
    ('solana_pipeline', 'bronze', 'wallet_trade_history'),
    ('solana_traders', 'public', 'trader_token_swaps'),
    ('solana_trending', 'bronze', 'wallet_trade_history')
]

def get_table_schema(conn, schema_name, table_name):
    """Get detailed schema information for a table"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get column information
        cur.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name))
        
        columns = cur.fetchall()
        
        # Get row count
        cur.execute(f'SELECT COUNT(*) as count FROM {schema_name}.{table_name}')
        row_count = cur.fetchone()['count']
        
        # Get sample data
        cur.execute(f'SELECT * FROM {schema_name}.{table_name} LIMIT 1')
        sample_data = cur.fetchone()
        
        # Get indexes
        cur.execute("""
            SELECT 
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
        """, (schema_name, table_name))
        indexes = cur.fetchall()
        
        return {
            'columns': columns,
            'row_count': row_count,
            'sample_data': dict(sample_data) if sample_data else None,
            'indexes': indexes
        }

def analyze_schemas():
    """Analyze all PostgreSQL schemas"""
    results = {}
    
    for db_name, schema_name, table_name in TABLES_TO_ANALYZE:
        full_name = f"{db_name}.{schema_name}.{table_name}"
        print(f"\nAnalyzing {full_name}...")
        
        try:
            # Connect to database
            conn = psycopg2.connect(**DATABASES[db_name])
            
            # Get schema information
            schema_info = get_table_schema(conn, schema_name, table_name)
            results[full_name] = schema_info
            
            # Print summary
            print(f"  Columns: {len(schema_info['columns'])}")
            print(f"  Rows: {schema_info['row_count']:,}")
            
            conn.close()
            
        except Exception as e:
            print(f"  ERROR: {str(e)}")
            results[full_name] = {'error': str(e)}
    
    return results

def print_schema_comparison(results):
    """Print detailed schema comparison"""
    print("\n" + "="*80)
    print("DETAILED SCHEMA ANALYSIS")
    print("="*80)
    
    for table_name, info in results.items():
        print(f"\n### {table_name}")
        
        if 'error' in info:
            print(f"ERROR: {info['error']}")
            continue
        
        print(f"Row Count: {info['row_count']:,}")
        print(f"\nColumns ({len(info['columns'])}):")
        
        for col in info['columns']:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            dtype = col['data_type']
            if col['character_maximum_length']:
                dtype += f"({col['character_maximum_length']})"
            elif col['numeric_precision']:
                dtype += f"({col['numeric_precision']},{col['numeric_scale'] or 0})"
            
            print(f"  - {col['column_name']}: {dtype} {nullable}")
            if col['column_default']:
                print(f"    Default: {col['column_default']}")
        
        if info['indexes']:
            print(f"\nIndexes:")
            for idx in info['indexes']:
                print(f"  - {idx['indexname']}")
        
        if info['sample_data']:
            print(f"\nSample Data:")
            for key, value in list(info['sample_data'].items())[:5]:
                print(f"  {key}: {value}")

def find_commonalities(results):
    """Find common columns across tables"""
    print("\n" + "="*80)
    print("COMMON COLUMNS ANALYSIS")
    print("="*80)
    
    # Extract all column names by table
    table_columns = {}
    for table_name, info in results.items():
        if 'error' not in info:
            table_columns[table_name] = {col['column_name'] for col in info['columns']}
    
    # Find columns that appear in multiple tables
    all_columns = {}
    for table, columns in table_columns.items():
        for col in columns:
            if col not in all_columns:
                all_columns[col] = []
            all_columns[col].append(table)
    
    # Print common columns
    print("\nColumns appearing in multiple tables:")
    for col, tables in sorted(all_columns.items()):
        if len(tables) > 1:
            print(f"\n{col}: ({len(tables)} tables)")
            for table in tables:
                print(f"  - {table}")

if __name__ == "__main__":
    print("PostgreSQL Schema Analysis for MinIO Migration")
    print("Timestamp:", datetime.now().isoformat())
    
    # Analyze schemas
    results = analyze_schemas()
    
    # Print detailed comparison
    print_schema_comparison(results)
    
    # Find commonalities
    find_commonalities(results)
    
    # Save results to JSON
    output_file = f"/home/jgupdogg/dev/claude_pipeline/temp_analysis/postgres_schemas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n\nResults saved to: {output_file}")