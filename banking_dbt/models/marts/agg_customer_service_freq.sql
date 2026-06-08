{{ config(materialized='table') }}

-- Per-customer frequency of each service (how often a customer uses a service)
SELECT
    customer_id,
    service_id,
    service_name,
    category,
    COUNT(*)        AS total_uses,
    MIN(used_at)    AS first_used_at,
    MAX(used_at)    AS last_used_at
FROM {{ ref('fct_service_usage') }}
GROUP BY 1, 2, 3, 4
