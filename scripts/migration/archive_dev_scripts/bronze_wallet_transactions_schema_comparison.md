# Bronze Wallet Transactions Schema Comparison

**Analysis Date**: June 19, 2025  
**Current Schema Directory**: `s3://solana-data/bronze/wallet_transactions/`  
**Old Schema Directory**: `s3://solana-data/bronze/wallet_transactions_old_schema/`

## Executive Summary

The bronze wallet transactions schema underwent a **significant structural change** rather than a simple evolution. The current implementation uses **two different file types**:

1. **Status Files**: Small metadata files (4KB) containing batch processing information
2. **Transaction Data Files**: Large files (300KB-13MB) containing actual wallet transaction data

The old schema stored all data in a single comprehensive format, while the new schema separates concerns between status tracking and transaction data storage.

## File Structure Analysis

### Current Schema (27 files total)
- **Status Files**: `status_BATCH_ID.parquet` (4KB each)
- **Transaction Files**: `wallet_transactions_BATCH_ID.parquet` (18KB - 13MB)
- **Recent Large File**: 12.9MB file from `2025-06-19` with 30-column schema

### Old Schema (9 files total)
- **Single Format**: `wallet_transactions_BATCH_ID.parquet` (326KB each)
- **Comprehensive Data**: All transaction and metadata in single files

## Schema Comparison

### Status Files Schema (Current - 6 columns)

| Column | Type | Description |
|--------|------|-------------|
| `batch_id` | VARCHAR | Batch processing identifier |
| `processed_wallets_count` | BIGINT | Number of wallets processed |
| `total_transactions` | BIGINT | Total transactions in batch |
| `timestamp` | TIMESTAMP WITH TIME ZONE | Processing timestamp |
| `schema_version` | VARCHAR | Schema version identifier |
| `date` | DATE | Processing date |

### Transaction Data Schema (Current - 30 columns)

| Column | Type | Description |
|--------|------|-------------|
| `wallet_address` | VARCHAR | Wallet identifier |
| `transaction_hash` | VARCHAR | Transaction hash |
| `timestamp` | TIMESTAMP WITH TIME ZONE | Transaction timestamp |
| `base_symbol` | VARCHAR | Base token symbol |
| `base_address` | VARCHAR | Base token address |
| `base_type_swap` | VARCHAR | Base token swap type |
| `base_ui_change_amount` | DOUBLE | Base token UI amount change |
| `base_nearest_price` | DOUBLE | Base token price |
| `base_decimals` | INTEGER | Base token decimals |
| `base_ui_amount` | DOUBLE | Base token UI amount |
| `base_change_amount` | VARCHAR | Base token raw amount change |
| `quote_symbol` | VARCHAR | Quote token symbol |
| `quote_address` | VARCHAR | Quote token address |
| `quote_type_swap` | VARCHAR | Quote token swap type |
| `quote_ui_change_amount` | DOUBLE | Quote token UI amount change |
| `quote_nearest_price` | DOUBLE | Quote token price |
| `quote_decimals` | INTEGER | Quote token decimals |
| `quote_ui_amount` | DOUBLE | Quote token UI amount |
| `quote_change_amount` | VARCHAR | Quote token raw amount change |
| `source` | VARCHAR | Transaction source |
| `tx_type` | VARCHAR | Transaction type |
| `block_unix_time` | BIGINT | Block timestamp |
| `owner` | VARCHAR | Wallet owner |
| `processed_for_pnl` | BOOLEAN | PnL processing flag |
| `pnl_processed_at` | TIMESTAMP WITH TIME ZONE | PnL processing timestamp |
| `pnl_processing_batch_id` | VARCHAR | PnL batch identifier |
| `fetched_at` | TIMESTAMP WITH TIME ZONE | Data fetch timestamp |
| `batch_id` | VARCHAR | Batch identifier |
| `data_source` | VARCHAR | Data source identifier |
| `date` | DATE | Transaction date |

### Old Schema (28 columns)

| Column | Type | Description |
|--------|------|-------------|
| `wallet_address` | VARCHAR | Wallet identifier |
| `token_address` | VARCHAR | Token address |
| `transaction_hash` | VARCHAR | Transaction hash |
| `source` | VARCHAR | Transaction source |
| `block_unix_time` | BIGINT | Block timestamp |
| `tx_type` | VARCHAR | Transaction type |
| `timestamp` | TIMESTAMP WITH TIME ZONE | Transaction timestamp |
| `transaction_type` | VARCHAR | Transaction classification |
| `from_symbol` | VARCHAR | From token symbol |
| `from_address` | VARCHAR | From token address |
| `from_decimals` | INTEGER | From token decimals |
| `from_amount` | DOUBLE | From token amount |
| `from_raw_amount` | VARCHAR | From token raw amount |
| `to_symbol` | VARCHAR | To token symbol |
| `to_address` | VARCHAR | To token address |
| `to_decimals` | INTEGER | To token decimals |
| `to_amount` | DOUBLE | To token amount |
| `to_raw_amount` | VARCHAR | To token raw amount |
| `base_price` | DOUBLE | Base token price |
| `quote_price` | DOUBLE | Quote token price |
| `value_usd` | DOUBLE | USD value |
| `processed_for_pnl` | BOOLEAN | PnL processing flag |
| `pnl_processed_at` | TIMESTAMP WITH TIME ZONE | PnL processing timestamp |
| `pnl_processing_status` | VARCHAR | PnL processing status |
| `fetched_at` | TIMESTAMP WITH TIME ZONE | Data fetch timestamp |
| `batch_id` | VARCHAR | Batch identifier |
| `data_source` | VARCHAR | Data source identifier |
| `date` | DATE | Transaction date |

## Key Changes Analysis

### 🔄 **Schema Restructuring (Major Change)**

**From Single-Format to Dual-Format**:
- **Old**: All data in single comprehensive files
- **New**: Separated into status files + transaction data files

### ✅ **Enhanced Data Model (Current Schema)**

**Improved Token Representation**:
- **Old**: Simple `from_*/to_*` model (directional)
- **New**: `base_*/quote_*` model (base/quote pair paradigm)

**Enhanced Swap Information**:
- **Added**: `base_type_swap`, `quote_type_swap` (swap direction indicators)
- **Added**: `owner` field (wallet owner tracking)
- **Added**: `pnl_processing_batch_id` (improved batch tracking)

### ❌ **Removed Fields**

**Simplified Transaction Classification**:
- **Removed**: `transaction_type` (BUY/SELL classification)
- **Removed**: `value_usd` (USD value calculation)
- **Removed**: `pnl_processing_status` (status tracking)

**Price Model Changes**:
- **Removed**: `base_price`, `quote_price` (separate price fields)
- **Added**: `base_nearest_price`, `quote_nearest_price` (nearest price model)

### 🆕 **New Status Tracking**

**Batch Metadata**:
- **Added**: `processed_wallets_count` (processing metrics)
- **Added**: `total_transactions` (volume metrics)
- **Added**: `schema_version` (version tracking)

## Data Quality Observations

### Current Schema Advantages
1. **Better Separation of Concerns**: Status tracking vs transaction data
2. **Improved Token Model**: Base/quote paradigm aligns with trading terminology  
3. **Enhanced Metadata**: Better batch and processing tracking
4. **Scalable Architecture**: Large transaction files (13MB) indicate higher volume handling

### Potential Concerns
1. **Increased Complexity**: Two file types require coordinated processing
2. **Lost USD Valuation**: No direct USD value calculation in current schema
3. **Missing Transaction Classification**: No BUY/SELL classification
4. **Price Model Change**: Different price calculation methodology

## Data Volume Analysis

### File Size Progression
- **Old Schema**: ~326KB per file (consistent size)
- **Current Schema**: 18KB → 13MB (significant growth)
- **Recent Growth**: 12.9MB file indicates substantial data volume increase

### Processing Scale
- **Status Example**: 902 wallets, 71,325 transactions in single batch
- **Data Growth**: Files grew from ~18KB to ~13MB over time

## Migration Impact Assessment

### ✅ **Successfully Migrated**
- Core transaction data (wallet, hash, timestamp)
- Processing metadata (batch_id, fetched_at, date)
- PnL tracking infrastructure (processed_for_pnl, pnl_processed_at)

### ⚠️ **Requires Downstream Updates**
- Silver layer transformations expecting old column names
- PnL calculations relying on `from_amount`/`to_amount` vs `base_ui_amount`/`quote_ui_amount`
- USD value calculations missing direct `value_usd` field
- Transaction type classification logic (BUY/SELL determination)

### 🔧 **Recommended Actions**
1. **Update Silver Layer**: Modify transformations for new base/quote model
2. **Restore USD Calculation**: Implement USD value derivation logic
3. **Add Transaction Classification**: Implement BUY/SELL logic using base/quote amounts
4. **Validate PnL Pipeline**: Ensure compatibility with new amount fields

## Conclusion

The schema change represents a **comprehensive architectural evolution** from a simple directional model (`from/to`) to a sophisticated trading model (`base/quote`). While this improves data modeling for trading analytics, it requires significant downstream pipeline updates to maintain compatibility.

The separation into status and transaction files provides better scalability and monitoring capabilities, evidenced by the growth from 326KB to 13MB files, indicating successful handling of increased data volumes.

**Impact Level**: **HIGH** - Requires coordinated updates across silver/gold layers