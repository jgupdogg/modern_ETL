{% macro parse_solana_transaction(payload_column) %}
    CASE 
        WHEN JSON_VALID({{ payload_column }}) THEN
            CASE
                WHEN JSON_EXTRACT({{ payload_column }}, '$.transactionType') = 'SWAP' THEN 'solana_swap'
                WHEN JSON_EXTRACT({{ payload_column }}, '$.transaction') IS NOT NULL THEN 'solana_transaction'
                ELSE 'unknown'
            END
        ELSE 'invalid_json'
    END
{% endmacro %}

{% macro extract_transaction_amount(payload_json) %}
    CASE 
        WHEN {{ payload_json }} IS NOT NULL THEN
            CAST(JSON_EXTRACT({{ payload_json }}, '$.transactionAmount') AS DECIMAL)
        ELSE NULL
    END
{% endmacro %}

{% macro validate_solana_signature(signature_field) %}
    CASE 
        WHEN {{ signature_field }} IS NOT NULL 
        AND LENGTH({{ signature_field }}) = 88  -- Standard Solana signature length
        THEN true
        ELSE false
    END
{% endmacro %}