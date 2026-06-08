{{ config(materialized='view') }}

with ranked as (
    select
        v:id::int            as usage_id,
        v:customer_id::int   as customer_id,
        v:account_id::int    as account_id,
        v:service_id::int    as service_id,
        v:channel::string    as channel,
        v:status::string     as status,
        v:used_at::timestamp as used_at,
        current_timestamp     as load_timestamp,
        row_number() over (
            partition by v:id::int
            order by v:used_at desc
        ) as rn
    from {{ source('raw', 'service_usage') }}
)

select
    usage_id,
    customer_id,
    account_id,
    service_id,
    channel,
    status,
    used_at,
    load_timestamp
from ranked
where rn = 1
