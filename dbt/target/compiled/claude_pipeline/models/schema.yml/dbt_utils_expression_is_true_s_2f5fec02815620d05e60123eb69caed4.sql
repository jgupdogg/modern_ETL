



select
    1
from "analytics"."main"."silver_tracked_tokens"

where not(rank >= 1 AND "analytics"."main_dbt_test__audit"."dbt_utils_expression_is_true_s_2f5fec02815620d05e60123eb69caed4".rank <= 50)

