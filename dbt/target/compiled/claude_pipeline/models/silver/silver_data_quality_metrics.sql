

-- Silver layer: Data quality metrics by processing date
SELECT 
    processing_date,
    COUNT(*) as total_events,
    COUNT(CASE WHEN has_valid_json THEN 1 END) as valid_json_events,
    COUNT(CASE WHEN has_source_ip THEN 1 END) as events_with_source_ip,
    COUNT(CASE WHEN event_timestamp IS NOT NULL THEN 1 END) as events_with_valid_timestamp,
    COUNT(CASE WHEN event_type = 'unknown' THEN 1 END) as unknown_event_types,
    COUNT(DISTINCT message_id) as unique_message_ids,
    
    -- Quality percentages
    ROUND(COUNT(CASE WHEN has_valid_json THEN 1 END) * 100.0 / COUNT(*), 2) as json_validity_pct,
    ROUND(COUNT(CASE WHEN event_timestamp IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as timestamp_validity_pct,
    ROUND(COUNT(CASE WHEN event_type != 'unknown' THEN 1 END) * 100.0 / COUNT(*), 2) as categorization_pct,
    
    -- Event type distribution
    COUNT(CASE WHEN event_type = 'solana_swap' THEN 1 END) as solana_swap_count,
    COUNT(CASE WHEN event_type = 'solana_transaction' THEN 1 END) as solana_transaction_count,
    COUNT(CASE WHEN event_type = 'test_event' THEN 1 END) as test_event_count,
    
    -- Processing metadata
    MIN(silver_processed_at) as first_processed,
    MAX(silver_processed_at) as last_processed,
    CURRENT_TIMESTAMP as metrics_calculated_at
    
FROM "analytics"."main"."silver_webhook_events"


  -- Only calculate metrics for new processing dates
  WHERE processing_date > (SELECT MAX(processing_date) FROM "analytics"."main"."silver_data_quality_metrics")


GROUP BY processing_date
ORDER BY processing_date