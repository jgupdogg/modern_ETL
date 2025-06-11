
        
            delete from "analytics"."main"."silver_data_quality_metrics"
            where (
                processing_date) in (
                select (processing_date)
                from "silver_data_quality_metrics__dbt_tmp20250611125631537224"
            );

        
    

    insert into "analytics"."main"."silver_data_quality_metrics" ("processing_date", "total_events", "valid_json_events", "events_with_source_ip", "events_with_valid_timestamp", "unknown_event_types", "unique_message_ids", "json_validity_pct", "timestamp_validity_pct", "categorization_pct", "solana_swap_count", "solana_transaction_count", "test_event_count", "first_processed", "last_processed", "metrics_calculated_at")
    (
        select "processing_date", "total_events", "valid_json_events", "events_with_source_ip", "events_with_valid_timestamp", "unknown_event_types", "unique_message_ids", "json_validity_pct", "timestamp_validity_pct", "categorization_pct", "solana_swap_count", "solana_transaction_count", "test_event_count", "first_processed", "last_processed", "metrics_calculated_at"
        from "silver_data_quality_metrics__dbt_tmp20250611125631537224"
    )
  