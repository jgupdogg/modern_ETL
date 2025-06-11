



select
    1
from "analytics"."main"."silver_data_quality_metrics"

where not(total_events >= 0)

