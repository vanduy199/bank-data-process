{{ config(materialized='incremental', unique_key='transaction_id') }}

with dedup as (
    select
        t.*,
        row_number() over (partition by t.transaction_id order by t.transaction_time desc) as rn
    from {{ ref('stg_transactions') }} t
)

SELECT
    d.transaction_id,
    d.account_id,
    a.customer_id,
    d.amount,
    d.related_account_id,
    d.status,
    d.transaction_type,
    d.transaction_time,
    CURRENT_TIMESTAMP AS load_timestamp
FROM dedup d
LEFT JOIN {{ ref('stg_accounts') }} a
    ON d.account_id = a.account_id
WHERE d.rn = 1
