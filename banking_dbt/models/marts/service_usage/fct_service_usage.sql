{{ config(materialized='incremental', unique_key='usage_id') }}

-- One row per service-usage event, enriched with current service attributes
SELECT
    u.usage_id,
    u.customer_id,
    u.account_id,
    u.service_id,
    s.service_code,
    s.service_name,
    s.category,
    u.channel,
    u.status,
    u.used_at,
    u.used_at::date AS usage_date,
    CURRENT_TIMESTAMP AS load_timestamp
FROM {{ ref('stg_service_usage') }} u
LEFT JOIN {{ ref('dim_services') }} s
    ON u.service_id = s.service_id
    AND s.is_current = TRUE

{% if is_incremental() %}
WHERE u.usage_id > (SELECT COALESCE(MAX(usage_id), 0) FROM {{ this }})
{% endif %}
