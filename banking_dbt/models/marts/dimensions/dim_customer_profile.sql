{{
  config(
    materialized='incremental',
    unique_key='customer_id'
  )
}}

-- Dimension: customer profile (one row per customer with latest metrics)

with metrics as (
  select * from {{ ref('int_customer_metrics') }}
)

select
  customer_id,
  total_transactions,
  total_transfer_amount,
  avg_transaction_amount,
  login_frequency,
  preferred_transaction_type,
  favorite_feature,
  last_active_date,
  risk_score,
  updated_at
from metrics

{% if is_incremental() %}
  where updated_at > (select coalesce(max(updated_at), '1970-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
