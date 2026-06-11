{{ config(materialized='table') }}

-- Service dimension built from the SCD2 snapshot (full history + current flag)
WITH source_data AS (
    SELECT
        service_id,
        service_code,
        service_name,
        category,
        is_active,
        created_at,
        dbt_valid_from   AS effective_from,
        dbt_valid_to     AS effective_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('services_snapshot') }}
)

SELECT * FROM source_data
