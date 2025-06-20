"""
Smart Trader Pipeline Configuration
Centralized settings for filters, API limits, and thresholds
"""

# =============================================================================
# API CONFIGURATION & LIMITS
# =============================================================================

# BirdEye API Rate Limiting
API_RATE_LIMIT_DELAY = 0.5          # Seconds between API calls
WALLET_API_DELAY = 1.0               # Seconds between wallet API calls
API_PAGINATION_LIMIT = 100           # Records per API call

# Batch Processing Limits
BRONZE_TOKEN_BATCH_LIMIT = 100       # Max tokens to fetch per run
BRONZE_WHALE_BATCH_LIMIT = 50        # Max tokens to process for whales per run  
BRONZE_WALLET_BATCH_LIMIT = 20     # Max wallets to process per run (increased for full processing)
SILVER_PNL_BATCH_LIMIT = 10000       # Max wallets for PnL calculation per run (increased for full processing)
GOLD_MAX_TRADERS_PER_BATCH = 1000     # Max top traders per batch

# =============================================================================
# BRONZE LAYER FILTERS
# =============================================================================

# Token List Filtering Criteria
TOKEN_LIMIT = 100                    # Total tokens to fetch
MIN_LIQUIDITY = 200000               # Minimum token liquidity
MAX_LIQUIDITY = 1000000              # Maximum token liquidity  
MIN_VOLUME_1H_USD = 200000           # Minimum 1-hour volume
MIN_PRICE_CHANGE_2H_PERCENT = 10     # Minimum 2-hour price change %
MIN_PRICE_CHANGE_24H_PERCENT = 30    # Minimum 24-hour price change %

# Whale Data Settings
MAX_WHALES_PER_TOKEN = 100            # Top N holders to fetch per token
WHALE_REFRESH_DAYS = 7               # Re-fetch whale data if older than N days

# Transaction History Settings  
MAX_TRANSACTIONS_PER_WALLET = 100    # Transaction history depth per wallet

# =============================================================================
# SILVER LAYER FILTERS
# =============================================================================

# Tracked Token Selection (Reduced for testing - can be increased for production)
TRACKED_TOKEN_LIMIT = 50             # Max tokens to track in silver layer
SILVER_MIN_LIQUIDITY = 1000          # Minimum liquidity for tracked tokens
SILVER_MIN_VOLUME = 1000             # Minimum volume for tracked tokens
SILVER_MIN_VOLUME_MCAP_RATIO = 0.001 # Minimum volume/market cap ratio
SILVER_MIN_PRICE_CHANGE = 1          # Minimum price change % for tracking

# PnL Calculation Timeframes
PNL_TIMEFRAMES = ['all', 'week', 'month', 'quarter']
PNL_WEEK_DAYS = 7
PNL_MONTH_DAYS = 30  
PNL_QUARTER_DAYS = 90

# =============================================================================
# SILVER PNL PROCESSING LIMITS
# =============================================================================

# Transaction Selection Criteria
SILVER_PNL_RECENT_DAYS = 7            # Include transactions from last N days
SILVER_PNL_HISTORICAL_LIMIT = 100     # Maximum total transactions per wallet
SILVER_PNL_MIN_TRANSACTIONS = 5       # Skip wallets with too few trades

# PnL Calculation Precision
PNL_AMOUNT_PRECISION_THRESHOLD = 0.001  # Minimum amount threshold for calculations
PNL_CALCULATION_PRECISION = 6          # Decimal places for PnL calculations

# Processing Performance
PNL_BATCH_PROGRESS_INTERVAL = 10      # Log progress every N wallets
PNL_MAX_PROCESSING_TIME_MINUTES = 30  # Timeout for PnL calculations

# Wallet Batch Processing (Memory Management)
SILVER_PNL_WALLET_BATCH_SIZE = 5      # Process N wallets at a time to control memory usage
SILVER_PNL_MAX_TRANSACTIONS_PER_BATCH = 500  # Limit transaction volume per batch
SILVER_PNL_PROCESSING_TIMEOUT_SECONDS = 300  # 5 minutes timeout per wallet batch

# =============================================================================
# GOLD LAYER THRESHOLDS
# =============================================================================

# Minimum Profitability Criteria (Base Requirements)
MIN_TOTAL_PNL = 10.0                 # Minimum profit in USD (realistic threshold)
MIN_ROI_PERCENT = 1.0                # Minimum ROI percentage
MIN_WIN_RATE_PERCENT = 40.0          # Minimum win rate percentage
MIN_TRADE_COUNT = 10                  # Minimum number of trades (adjusted for current data)

# Performance Tier Thresholds
# Elite Tier
ELITE_MIN_PNL = 1000                 # $1000+ profit
ELITE_MIN_ROI = 30                   # 30%+ ROI  
ELITE_MIN_WIN_RATE = 60              # 60%+ win rate
ELITE_MIN_TRADES = 10                # 10+ trades

# Strong Tier  
STRONG_MIN_PNL = 100                 # $100+ profit
STRONG_MIN_ROI = 15                  # 15%+ ROI
STRONG_MIN_WIN_RATE = 40             # 40%+ win rate
STRONG_MIN_TRADES = 5                # 5+ trades

# Gold Layer Processing Limits
GOLD_MAX_TRADERS_PER_BATCH = 100     # Maximum traders per batch
PERFORMANCE_LOOKBACK_DAYS = 30       # Days to look back for performance

# (Promising Tier = anything that meets base requirements)

# =============================================================================
# HELIUS INTEGRATION
# =============================================================================

# Helius API Settings
HELIUS_API_BASE_URL = "https://api.helius.xyz/v0"
HELIUS_MAX_ADDRESSES = 100           # Max addresses per webhook (API limit)
HELIUS_WEBHOOK_TYPE = "enhanced"     # Webhook type
HELIUS_TRANSACTION_TYPES = ["SWAP", "TRANSFER"]  # Transaction types to monitor
HELIUS_REQUEST_TIMEOUT = 30          # Request timeout in seconds

# Performance tier priority for webhook updates
HELIUS_TIER_PRIORITY = ["elite", "strong", "promising"]

# =============================================================================
# INFRASTRUCTURE
# =============================================================================

# MinIO/S3 Configuration  
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin123'
MINIO_BUCKET = 'solana-data'

# PySpark Configuration
SPARK_DRIVER_MEMORY = '4g'  # Increased from 2g for large dataset processing
SPARK_EXECUTOR_MEMORY = '4g'  # Increased from 2g for large dataset processing
SPARK_DRIVER_MAX_RESULT_SIZE = '2g'  # Prevent driver OOM errors
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367"

# Storage Paths
BRONZE_TOKEN_LIST_PATH = "bronze/token_list_v3"
BRONZE_TOKEN_WHALES_PATH = "bronze/token_whales"  
BRONZE_WALLET_TRANSACTIONS_PATH = "bronze/wallet_transactions"  # Clean deduplicated data (primary)
BRONZE_WALLET_TRANSACTIONS_WITH_DUPES_PATH = "bronze/wallet_transactions_with_dupes"  # Archived duplicate data
SILVER_TRACKED_TOKENS_PATH = "silver/tracked_tokens"
SILVER_WALLET_PNL_PATH = "silver/wallet_pnl"
GOLD_TOP_TRADERS_PATH = "gold/top_traders"

# Required Airflow Variables (for reference)
REQUIRED_AIRFLOW_VARIABLES = [
    'BIRDSEYE_API_KEY',      # BirdEye API authentication
    'HELIUS_API_KEY',        # Helius API authentication  
    'HELIUS_WEBHOOK_URL'     # Target webhook URL
]

# Solana Constants
SOL_TOKEN_ADDRESS = "So11111111111111111111111111111111111111112"

# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_spark_config():
    """Get PySpark configuration dictionary with memory optimization and JVM tuning"""
    return {
        "spark.jars.packages": SPARK_PACKAGES,
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        
        # Memory Configuration (Increased for large datasets)
        "spark.driver.memory": SPARK_DRIVER_MEMORY,
        "spark.executor.memory": SPARK_EXECUTOR_MEMORY,
        "spark.driver.maxResultSize": SPARK_DRIVER_MAX_RESULT_SIZE,
        
        # Conservative Parallelism (Reduced for memory control)
        "spark.default.parallelism": "2",
        "spark.sql.adaptive.enabled": "false",  # Disable adaptive for predictable memory usage
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "false",
        
        # JVM Tuning for Garbage Collection
        "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UnlockExperimentalVMOptions",
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UnlockExperimentalVMOptions",
        
        # Serialization and Performance
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        
        # Connection Stability
        "spark.hadoop.fs.s3a.connection.timeout": "30000",  # 30 second timeout
        "spark.hadoop.fs.s3a.retry.limit": "3",
        "spark.network.timeout": "600s"  # 10 minute network timeout
    }

def get_s3_path(path_suffix):
    """Get full S3 path for a dataset"""
    return f"s3a://{MINIO_BUCKET}/{path_suffix}/"

# =============================================================================
# DAG CONFIGURATION
# =============================================================================

# DAG Scheduling & Execution
DAG_SCHEDULE_INTERVAL = '0 9,21 * * *'  # 9 AM & 9 PM UTC
DAG_MAX_ACTIVE_RUNS = 1
DAG_CATCHUP = False
DAG_RETRIES = 2
DAG_RETRY_DELAY_MINUTES = 10
DAG_START_DAYS_AGO = 1

# DAG Metadata
DAG_OWNER = 'data-team'
DAG_DEPENDS_ON_PAST = False
DAG_EMAIL_ON_FAILURE = False
DAG_EMAIL_ON_RETRY = False
DAG_TAGS = ['end-to-end', 'smart-traders', 'medallion', 'analytics']

# =============================================================================
# DBT CONFIGURATION
# =============================================================================

# dbt Paths and Execution
DBT_PROFILES_DIR = '/opt/airflow/dbt'
DBT_PROJECT_DIR = '/opt/airflow/dbt'
DBT_MODEL_NAME = 'smart_wallets'

# =============================================================================
# VALIDATION CONFIGURATION
# =============================================================================

# DuckDB Validation Settings
DUCKDB_CONTAINER_NAME = 'claude_pipeline-duckdb'
DUCKDB_DATABASE_PATH = '/data/analytics.duckdb'

# =============================================================================
# ERROR CODE PATTERNS
# =============================================================================

# API Error Codes for Error Handling
API_RATE_LIMIT_CODES = ['429']
API_AUTH_ERROR_CODES = ['401', '403']
API_NOT_FOUND_CODES = ['404']
API_TIMEOUT_KEYWORDS = ['timeout']
API_RATE_LIMIT_KEYWORDS = ['rate limit']
API_AUTH_KEYWORDS = ['auth']

# Data Processing Error Keywords
DATA_EMPTY_KEYWORDS = ['no data', 'empty']
DATA_THRESHOLD_KEYWORDS = ['threshold']
PYSPARK_ERROR_KEYWORDS = ['pyspark', 'spark']
STORAGE_ERROR_KEYWORDS = ['s3', 'minio']
MEMORY_ERROR_KEYWORDS = ['out of memory', 'memory']
DBT_ERROR_KEYWORDS = ['dbt']
SQL_ERROR_KEYWORDS = ['sql', 'database']
NO_RESULTS_KEYWORDS = ['no silver data', 'no traders', 'no wallets', 'no tokens']

def get_duckdb_validation_command():
    """Get the DuckDB validation command for gold layer analytics"""
    return f"""
    docker exec {DUCKDB_CONTAINER_NAME} python3 -c "
import duckdb
conn = duckdb.connect('{DUCKDB_DATABASE_PATH}')
conn.execute('LOAD httpfs;')
conn.execute('SET s3_endpoint=\\'{MINIO_ENDPOINT}\\';')
conn.execute('SET s3_access_key_id=\\'{MINIO_ACCESS_KEY}\\';')
conn.execute('SET s3_secret_access_key=\\'{MINIO_SECRET_KEY}\\';')
conn.execute('SET s3_use_ssl=false;')
conn.execute('SET s3_url_style=\\'path\\';')

try:
    # Get smart wallet counts
    smart_wallets_count = conn.execute('SELECT COUNT(DISTINCT wallet_address) FROM smart_wallets;').fetchone()[0]
    
    # Get performance tier breakdown
    tier_counts = conn.execute('SELECT performance_tier, COUNT(*) as count FROM smart_wallets GROUP BY performance_tier;').fetchall()
    tier_dict = {{row[0]: row[1] for row in tier_counts}}
    
    print(f'RESULT:{{\\\"count\\\":\\{{smart_wallets_count}},\\\"tiers\\\":\\{{tier_dict}}}}')
except Exception as e:
    print(f'ERROR:{{e}}')
conn.close()
"
    """