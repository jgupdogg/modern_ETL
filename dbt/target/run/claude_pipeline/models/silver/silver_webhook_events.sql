
        
            delete from "analytics"."main"."silver_webhook_events"
            where (
                message_id) in (
                select (message_id)
                from "silver_webhook_events__dbt_tmp20250611125631398095"
            );

        
    

    insert into "analytics"."main"."silver_webhook_events" ("message_id", "event_timestamp", "source_ip", "file_path", "payload_json", "payload_source", "payload_test", "payload_value", "event_type", "has_valid_json", "has_source_ip", "kafka_timestamp", "processed_at", "processing_date", "silver_processed_at")
    (
        select "message_id", "event_timestamp", "source_ip", "file_path", "payload_json", "payload_source", "payload_test", "payload_value", "event_type", "has_valid_json", "has_source_ip", "kafka_timestamp", "processed_at", "processing_date", "silver_processed_at"
        from "silver_webhook_events__dbt_tmp20250611125631398095"
    )
  