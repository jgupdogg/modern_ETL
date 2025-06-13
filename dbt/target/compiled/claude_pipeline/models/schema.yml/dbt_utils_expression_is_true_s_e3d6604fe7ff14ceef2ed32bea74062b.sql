



select
    1
from "analytics"."main"."silver_tracked_tokens"

where not(price24hChangePercent >= 30)

