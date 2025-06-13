



select
    1
from "analytics"."main"."silver_tracked_tokens"

where not(volume24hUSD >= 50000)

