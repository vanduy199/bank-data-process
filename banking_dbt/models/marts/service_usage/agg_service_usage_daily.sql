{{ config(materialized='table') }}

-- Daily usage trend per service and channel
SELECT
    usage_date,
    service_id,
    service_name,
    category,
    channel,
    COUNT(*)                     AS total_uses,
    COUNT(DISTINCT customer_id)  AS distinct_customers
FROM {{ ref('fct_service_usage') }}
GROUP BY 1, 2, 3, 4, 5
