



select
    1
from "analytics"."main"."silver_tracked_tokens"

where not(rank >= 1 AND rank <= 50)

