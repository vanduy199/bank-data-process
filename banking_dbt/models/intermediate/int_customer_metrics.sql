{{
  config(
    materialized='incremental',
    unique_key='customer_id'
  )
}}

-- Final per-customer metrics used to populate the dimension and for snapshots

with behavior as (
  select * from {{ ref('int_customer_behavior') }}
)

select
  b.customer_id,
  b.total_transactions,
  b.total_transfer_amount,
  b.avg_transaction_amount,
  b.login_frequency,
  b.preferred_transaction_type,
  b.favorite_feature,
  b.last_active_date,
  -- Normalize/derive a numeric risk score (0-100)
  greatest(0, least(100, round(coalesce(b.risk_score,0) + (coalesce(b.total_transfer_amount,0)/1000) + (coalesce(b.avg_transaction_amount,0)/10),2))) as risk_score,
  current_timestamp() as updated_at
from behavior b

{% if is_incremental() %}
  where b.last_transaction_time > (select coalesce(max(updated_at), '1970-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
