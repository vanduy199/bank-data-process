{{ config(materialized='view') }}

with ranked as (
    select
        v:id::int               as service_id,
        v:service_code::string  as service_code,
        v:service_name::string  as service_name,
        v:category::string      as category,
        v:is_active::boolean     as is_active,
        v:created_at::timestamp as created_at,
        current_timestamp        as load_timestamp,
        row_number() over (
            partition by v:id::int
            order by v:created_at desc
        ) as rn
    from {{ source('raw', 'services') }}
)

select
    service_id,
    service_code,
    service_name,
    category,
    is_active,
    created_at,
    load_timestamp
from ranked
where rn = 1
