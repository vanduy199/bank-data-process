{{ config(materialized='incremental', unique_key=['customer_id', 'activity_date']) }}

-- Fact table: customer activity (daily roll-ups)
-- Aggregates transaction activity per customer per day for analytics

with tx as (
  select
    a.customer_id,
    t.transaction_time::date as activity_date,
    t.amount
  from {{ ref('stg_transactions') }} t
  left join {{ ref('stg_accounts') }} a on t.account_id = a.account_id
),
agg as (
  select
    customer_id,
    activity_date,
    count(*) as transactions_count,
    sum(coalesce(amount,0)) as total_amount,
    avg(coalesce(amount,0)) as avg_amount
  from tx
  group by customer_id, activity_date
)

select
  customer_id,
  activity_date,
  transactions_count,
  total_amount,
  avg_amount
from agg

{% if is_incremental() %}
  where activity_date > (select coalesce(max(activity_date), '1970-01-01'::date) from {{ this }})
{% endif %}
