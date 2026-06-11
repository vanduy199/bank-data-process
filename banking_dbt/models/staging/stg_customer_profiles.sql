{{ config(materialized='view') }}

-- Staging model for raw `customer_profiles` landing table
-- Purpose: normalize types and provide a single source of truth for downstream models

with ranked as (
  select
    v:customer_id::string as customer_id,
    try_to_number(v:total_transactions::string) as total_transactions,
    try_to_number(v:total_transfer_amount::string) as total_transfer_amount,
    try_to_number(v:avg_transaction_amount::string) as avg_transaction_amount,
    try_to_number(v:login_frequency::string) as login_frequency,
    v:preferred_transaction_type::string as preferred_transaction_type,
    v:favorite_feature::string as favorite_feature,
    v:last_active_date::timestamp_ntz as last_active_date,
    try_to_number(v:risk_score::string) as risk_score,
    v:updated_at::timestamp_ntz as updated_at,
    current_timestamp() as load_ts,
    row_number() over (
      partition by v:customer_id::string
      order by v:updated_at desc nulls last
    ) as rn
  from {{ source('raw', 'customer_profiles') }}
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
  updated_at,
  load_ts
from ranked
where rn = 1
