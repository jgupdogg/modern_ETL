"""
Memory Monitoring Utilities for PySpark Operations
Helps prevent JVM crashes by checking memory before operations
"""

import logging
import psutil
import os
from typing import Dict, Tuple, Optional

logger = logging.getLogger(__name__)


def get_system_memory_info() -> Dict[str, float]:
    """Get current system memory statistics in MB"""
    memory = psutil.virtual_memory()
    return {
        "total_mb": memory.total / (1024 * 1024),
        "available_mb": memory.available / (1024 * 1024),
        "used_mb": memory.used / (1024 * 1024),
        "percent_used": memory.percent,
        "free_mb": memory.free / (1024 * 1024)
    }


def get_process_memory_info() -> Dict[str, float]:
    """Get current process memory usage in MB"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return {
        "rss_mb": memory_info.rss / (1024 * 1024),  # Resident Set Size
        "vms_mb": memory_info.vms / (1024 * 1024),  # Virtual Memory Size
        "percent": process.memory_percent()
    }


def check_memory_availability(required_mb: float = 1024) -> Tuple[bool, str]:
    """
    Check if sufficient memory is available for operation
    
    Args:
        required_mb: Required memory in MB (default 1GB)
        
    Returns:
        Tuple of (is_safe, message)
    """
    system_info = get_system_memory_info()
    process_info = get_process_memory_info()
    
    available_mb = system_info['available_mb']
    free_mb = system_info['free_mb']
    percent_used = system_info['percent_used']
    
    # Conservative checks
    if available_mb < required_mb:
        return False, f"Insufficient memory: {available_mb:.0f}MB available, {required_mb:.0f}MB required"
    
    if percent_used > 85:
        return False, f"System memory critically high: {percent_used:.1f}% used"
    
    if process_info['rss_mb'] > 500:  # Process using more than 500MB
        return False, f"Process memory high: {process_info['rss_mb']:.0f}MB RSS"
    
    return True, f"Memory OK: {available_mb:.0f}MB available, {percent_used:.1f}% system used"


def log_memory_status(context: str = ""):
    """Log current memory status with optional context"""
    system_info = get_system_memory_info()
    process_info = get_process_memory_info()
    
    logger.info(f"🔍 Memory Status {context}")
    logger.info(f"  System: {system_info['available_mb']:.0f}MB available ({system_info['percent_used']:.1f}% used)")
    logger.info(f"  Process: {process_info['rss_mb']:.0f}MB RSS ({process_info['percent']:.1f}% of system)")


def memory_guard(required_mb: float = 1024, operation: str = "PySpark operation"):
    """
    Decorator to check memory before executing operations
    
    Usage:
        @memory_guard(required_mb=512, operation="Delta Lake write")
        def my_spark_operation():
            # spark operations
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Check memory before operation
            is_safe, message = check_memory_availability(required_mb)
            
            if not is_safe:
                logger.error(f"❌ Memory check failed for {operation}: {message}")
                raise MemoryError(f"Insufficient memory for {operation}: {message}")
            
            logger.info(f"✅ Memory check passed for {operation}: {message}")
            log_memory_status(f"before {operation}")
            
            try:
                result = func(*args, **kwargs)
                log_memory_status(f"after {operation}")
                return result
            except Exception as e:
                log_memory_status(f"after failed {operation}")
                raise
                
        return wrapper
    return decorator


def estimate_dataframe_memory(row_count: int, column_count: int, avg_bytes_per_cell: int = 8) -> float:
    """
    Estimate memory required for a DataFrame
    
    Args:
        row_count: Number of rows
        column_count: Number of columns
        avg_bytes_per_cell: Average bytes per cell (default 8 for numeric)
        
    Returns:
        Estimated memory in MB
    """
    total_cells = row_count * column_count
    total_bytes = total_cells * avg_bytes_per_cell
    
    # Add overhead for metadata, indices, etc (roughly 2x)
    total_with_overhead = total_bytes * 2
    
    return total_with_overhead / (1024 * 1024)


def should_use_batch_processing(row_count: int, column_count: int, available_memory_mb: Optional[float] = None) -> Tuple[bool, int]:
    """
    Determine if batch processing is needed and suggest batch size
    
    Args:
        row_count: Total number of rows
        column_count: Number of columns
        available_memory_mb: Available memory (auto-detected if None)
        
    Returns:
        Tuple of (should_batch, suggested_batch_size)
    """
    if available_memory_mb is None:
        system_info = get_system_memory_info()
        available_memory_mb = system_info['available_mb']
    
    # Estimate memory needed
    estimated_mb = estimate_dataframe_memory(row_count, column_count)
    
    # Use only 25% of available memory for safety
    safe_memory_mb = available_memory_mb * 0.25
    
    if estimated_mb > safe_memory_mb:
        # Calculate safe batch size
        batch_size = int(row_count * (safe_memory_mb / estimated_mb))
        # Ensure minimum batch size of 100
        batch_size = max(100, batch_size)
        return True, batch_size
    
    return False, row_count


# Pre-flight check function for DAG tasks
def pre_flight_memory_check(task_name: str, required_mb: float = 512) -> Dict[str, any]:
    """
    Perform pre-flight memory check for a task
    
    Args:
        task_name: Name of the task
        required_mb: Required memory in MB
        
    Returns:
        Dict with check results
    """
    log_memory_status(f"pre-flight for {task_name}")
    
    is_safe, message = check_memory_availability(required_mb)
    system_info = get_system_memory_info()
    process_info = get_process_memory_info()
    
    result = {
        "task_name": task_name,
        "is_safe": is_safe,
        "message": message,
        "system_memory_mb": system_info['available_mb'],
        "process_memory_mb": process_info['rss_mb'],
        "memory_percent": system_info['percent_used']
    }
    
    if not is_safe:
        logger.warning(f"⚠️ Pre-flight check failed for {task_name}: {message}")
    else:
        logger.info(f"✅ Pre-flight check passed for {task_name}")
    
    return result