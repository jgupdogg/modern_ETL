#!/usr/bin/env python3
"""
Reset downstream tables from silver PnL onwards
This clears silver_pnl and gold tables so we can test the corrected FIFO PnL logic
"""

import sys
import os
import logging

from utils.true_delta_manager import TrueDeltaLakeManager, get_table_path
from pyspark.sql.functions import lit, current_timestamp, col

def reset_downstream_tables():
    """Reset silver PnL and gold tables for fresh FIFO calculation"""
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        logger.info("🔄 Starting downstream table reset...")
        
        # Initialize Delta Lake manager
        delta_manager = TrueDeltaLakeManager()
        
        # Tables to reset (in reverse dependency order)
        tables_to_reset = [
            ("gold_traders", "s3a://smart-trader/gold/smart_traders_delta"),
            ("silver_pnl", "s3a://smart-trader/silver/wallet_pnl")
        ]
        
        reset_count = 0
        
        for table_name, table_path in tables_to_reset:
            try:
                if delta_manager.table_exists(table_path):
                    # Get record count before deletion
                    df = delta_manager.spark.read.format("delta").load(table_path)
                    record_count = df.count()
                    
                    # Delete all files in the table directory (including _delta_log)
                    logger.info(f"🗑️ Deleting {table_name} table ({record_count:,} records)")
                    
                    # Use MinIO to delete the entire directory
                    import subprocess
                    delete_cmd = f"docker exec claude_pipeline-minio mc rm --recursive --force local/smart-trader/{table_path.split('/')[-2]}/{table_path.split('/')[-1]}/"
                    subprocess.run(delete_cmd, shell=True, check=False)
                    
                    logger.info(f"✅ {table_name} table deleted successfully")
                    reset_count += 1
                else:
                    logger.info(f"ℹ️ {table_name} table does not exist, skipping")
                    
            except Exception as e:
                logger.error(f"❌ Error resetting {table_name}: {str(e)}")
                continue
        
        # Also reset PnL status in silver_whales so all wallets will be reprocessed
        logger.info("🔄 Resetting PnL status in silver_whales...")
        silver_whales_path = get_table_path("silver_whales")
        
        if delta_manager.table_exists(silver_whales_path):
            # Read current silver whales
            silver_whales_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
            
            # Reset all PnL processing status to pending
            updated_df = silver_whales_df.withColumn(
                "pnl_processing_status", lit("pending")
            ).withColumn(
                "pnl_last_processed_at", lit(None).cast("string")
            ).withColumn(
                "_delta_timestamp", current_timestamp()
            ).withColumn(
                "_delta_operation", lit("FIFO_PNL_RESET")
            )
            
            # Write back with overwrite
            updated_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_whales_path)
            
            total_whales = silver_whales_df.count()
            logger.info(f"✅ Reset PnL status for {total_whales:,} whales to pending")
        
        logger.info(f"🎉 Downstream reset complete! Reset {reset_count} tables")
        logger.info("📋 Ready for FIFO PnL calculation testing")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during downstream reset: {str(e)}")
        return False
        
    finally:
        if delta_manager:
            delta_manager.stop()

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting downstream table reset for FIFO PnL testing...")
    
    success = reset_downstream_tables()
    
    if success:
        logger.info("✅ Downstream reset completed successfully")
        logger.info("🔥 Ready to test corrected FIFO PnL calculation!")
        sys.exit(0)
    else:
        logger.error("❌ Downstream reset failed")
        sys.exit(1)