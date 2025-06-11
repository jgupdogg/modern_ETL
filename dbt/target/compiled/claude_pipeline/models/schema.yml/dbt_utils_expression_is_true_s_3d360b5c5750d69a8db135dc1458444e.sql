



select
    1
from "analytics"."main"."silver_data_quality_metrics"

where not(json_validity_pct "analytics"."main_dbt_test__audit"."dbt_utils_expression_is_true_s_3d360b5c5750d69a8db135dc1458444e".json_validity_pct >= 0 AND "analytics"."main_dbt_test__audit"."dbt_utils_expression_is_true_s_3d360b5c5750d69a8db135dc1458444e".json_validity_pct <= 100)

