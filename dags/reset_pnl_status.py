#!/usr/bin/env python3
"""
Reset PnL calculation status for all whales in silver_whales table
This script resets all whales to pending status so PnL will be recalculated
"""

import sys
import os
import logging

from utils.true_delta_manager import TrueDeltaLakeManager, get_table_path
from pyspark.sql.functions import lit, current_timestamp, col

def reset_pnl_status():
    """Reset all whale PnL processing status to pending"""
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        logger.info("🔄 Starting PnL status reset...")
        
        # Initialize Delta Lake manager
        delta_manager = TrueDeltaLakeManager()
        
        # Get silver whales table path
        silver_whales_path = get_table_path("silver_whales")
        
        if not delta_manager.table_exists(silver_whales_path):
            logger.error("❌ Silver whales table does not exist")
            return False
        
        # Read current silver whales
        silver_whales_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
        
        # Get count before update
        total_whales = silver_whales_df.count()
        logger.info(f"📊 Total whales in silver table: {total_whales}")
        
        # Count whales that were marked as completed
        completed_count = silver_whales_df.filter(col("pnl_processing_status") == "completed").count()
        logger.info(f"✅ Whales previously marked as PnL completed: {completed_count}")
        
        # Reset all PnL processing status to pending
        updated_df = silver_whales_df.withColumn(
            "pnl_processing_status", lit("pending")
        ).withColumn(
            "pnl_last_processed_at", lit(None).cast("string")
        ).withColumn(
            "_delta_timestamp", current_timestamp()
        ).withColumn(
            "_delta_operation", lit("PNL_STATUS_RESET")
        )
        
        # Write back with overwrite
        updated_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_whales_path)
        
        logger.info(f"🔄 Reset {total_whales} whales to pending PnL status")
        logger.info("✅ All whales now marked as needing PnL calculation")
        
        # Verify the update
        verification_df = delta_manager.spark.read.format("delta").load(silver_whales_path)
        pending_count = verification_df.filter(col("pnl_processing_status") == "pending").count()
        logger.info(f"📋 Verification: {pending_count} whales now have pending PnL status")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error resetting PnL status: {str(e)}")
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
    logger.info("🚀 Starting PnL status reset script...")
    
    success = reset_pnl_status()
    
    if success:
        logger.info("✅ PnL status reset completed successfully")
        sys.exit(0)
    else:
        logger.error("❌ PnL status reset failed")
        sys.exit(1)