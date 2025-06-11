



select
    1
from "analytics"."main"."silver_data_quality_metrics"

where not(unique_message_ids <= total_events)

