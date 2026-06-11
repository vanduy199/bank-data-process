{{ config(materialized='table') }}

-- Intermediate aggregation of transactions at customer level
-- Produces counts, sums, averages and preferred transaction type

with tx as (
  select
    a.customer_id,
    t.transaction_type,
    t.amount,
    t.transaction_time
  from {{ ref('stg_transactions') }} t
  left join {{ ref('stg_accounts') }} a on t.account_id = a.account_id
),
type_counts as (
  select
    customer_id,
    transaction_type,
    count(*) as cnt,
    row_number() over (partition by customer_id order by count(*) desc) as rn
  from tx
  group by customer_id, transaction_type
),
aggregates as (
  select
    t.customer_id,
    count(*) as total_transactions,
    sum(case when lower(t.transaction_type) = 'transfer' then coalesce(t.amount,0) else 0 end) as total_transfer_amount,
    avg(coalesce(t.amount,0)) as avg_transaction_amount,
    max(t.transaction_time) as last_transaction_time
  from tx t
  group by t.customer_id
)

select
  agg.customer_id,
  agg.total_transactions,
  agg.total_transfer_amount,
  agg.avg_transaction_amount,
  agg.last_transaction_time,
  tc.transaction_type as preferred_transaction_type
from aggregates agg
left join type_counts tc on tc.customer_id = agg.customer_id and tc.rn = 1
