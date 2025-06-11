



select
    1
from "analytics"."main"."silver_data_quality_metrics"

where not(json_validity_pct json_validity_pct >= 0 AND json_validity_pct <= 100)

