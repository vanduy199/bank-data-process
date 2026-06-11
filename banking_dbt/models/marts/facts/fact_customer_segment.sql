{{ config(materialized='incremental', unique_key='customer_id') }}

-- Fact: latest segment assignment per customer
-- Historical changes are tracked via customer_segments_snapshot

with segments as (
  select * from {{ ref('stg_customer_segments') }}
)

select
  customer_id,
  segment_name,
  segment_score,
  assigned_at,
  updated_at
from (
  select *, row_number() over (partition by customer_id order by assigned_at desc) as rn
  from segments
) s
where s.rn = 1

{% if is_incremental() %}
  and assigned_at > (select coalesce(max(assigned_at), '1970-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
