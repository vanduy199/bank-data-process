{{ config(materialized='table') }}

-- "Tần suất sử dụng dịch vụ": overall ranking of how often each service is used
WITH usage AS (
    SELECT
        service_id,
        service_name,
        category,
        COUNT(*)                                         AS total_uses,
        COUNT(DISTINCT customer_id)                      AS distinct_customers,
        COUNT_IF(status = 'SUCCESS')                     AS successful_uses,
        COUNT_IF(status = 'FAILED')                      AS failed_uses,
        MAX(used_at)                                     AS last_used_at
    FROM {{ ref('fct_service_usage') }}
    GROUP BY 1, 2, 3
)

SELECT
    service_id,
    service_name,
    category,
    total_uses,
    distinct_customers,
    successful_uses,
    failed_uses,
    ROUND(100.0 * total_uses / NULLIF(SUM(total_uses) OVER (), 0), 2) AS pct_share,
    RANK() OVER (ORDER BY total_uses DESC)                            AS usage_rank,
    last_used_at
FROM usage
