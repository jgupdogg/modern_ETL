

-- Bronze layer: Deduplicated view of raw webhook data from MinIO
SELECT 
    message_id,
    timestamp as webhook_timestamp,
    source_ip,
    file_path,
    payload,
    kafka_timestamp,
    processed_at,
    processing_date,
    processing_hour,
    ROW_NUMBER() OVER (
        PARTITION BY message_id 
        ORDER BY processed_at DESC
    ) as row_num
FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
WHERE message_id IS NOT NULL