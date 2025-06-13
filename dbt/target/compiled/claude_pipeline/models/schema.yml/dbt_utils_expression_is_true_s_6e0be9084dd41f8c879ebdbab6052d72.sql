



select
    1
from "analytics"."main"."silver_tracked_tokens"

where not(quality_score >= 0 AND quality_score <= 1)

