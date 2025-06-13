{% macro update_bronze_processing_status() %}
  {% set update_query %}
    UPDATE bronze_tokens 
    SET 
      processing_status = 'silver_processed',
      silver_processed_at = CURRENT_TIMESTAMP
    WHERE processing_status = 'unprocessed'
      AND token_address IN (
        SELECT token_address 
        FROM silver_tracked_tokens 
        WHERE is_newly_tracked = true
      );
  {% endset %}
  
  {{ return(update_query) }}
{% endmacro %}

{% macro update_whale_fetch_status(token_addresses, status='fetched') %}
  {% set update_query %}
    UPDATE silver_tracked_tokens 
    SET 
      whale_fetch_status = '{{ status }}',
      whale_fetched_at = CURRENT_TIMESTAMP,
      is_newly_tracked = false
    WHERE token_address IN ({{ token_addresses }});
  {% endset %}
  
  {{ return(update_query) }}
{% endmacro %}