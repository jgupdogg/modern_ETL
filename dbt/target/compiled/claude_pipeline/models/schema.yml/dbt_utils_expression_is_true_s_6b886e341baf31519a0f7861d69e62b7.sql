



select
    1
from "analytics"."main"."silver_tracked_tokens"

where not(quality_score >= 0 AND "analytics"."main_dbt_test__audit"."dbt_utils_expression_is_true_s_6b886e341baf31519a0f7861d69e62b7".quality_score <= 1)

