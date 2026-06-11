{{ config(materialized='table') }}

-- Combines transaction aggregates with profile signals to compute behavior features

with tx as (
  select * from {{ ref('int_customer_transactions') }}
),
profiles as (
  select * from {{ ref('stg_customer_profiles') }}
)

select
  tx.customer_id,
  tx.total_transactions,
  tx.total_transfer_amount,
  tx.avg_transaction_amount,
  tx.last_transaction_time,
  coalesce(profiles.login_frequency, 0) as login_frequency,
  coalesce(profiles.preferred_transaction_type, tx.preferred_transaction_type) as preferred_transaction_type,
  profiles.favorite_feature,
  coalesce(profiles.last_active_date, tx.last_transaction_time) as last_active_date,
  coalesce(profiles.risk_score, 0) as risk_score
from tx
left join profiles on profiles.customer_id = tx.customer_id
