{{ config(materialized='view') }}

-- Staging model for raw `customer_segments` landing table
-- Purpose: normalize and expose segmentation assignments (latest per customer)

with ranked as (
  select
    v:customer_id::string as customer_id,
    v:segment_name::string as segment_name,
    try_to_number(v:segment_score::string) as segment_score,
    v:assigned_at::timestamp_ntz as assigned_at,
    current_timestamp() as load_ts,
    row_number() over (
      partition by v:customer_id::string
      order by v:assigned_at desc nulls last
    ) as rn
  from {{ source('raw', 'customer_segments') }}
)

select
  customer_id,
  segment_name,
  segment_score,
  assigned_at,
  assigned_at as updated_at,
  load_ts
from ranked
where rn = 1
