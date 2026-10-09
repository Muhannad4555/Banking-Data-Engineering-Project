with parsed as (
    select
        coalesce(event:after.id, event:before.id)::int as id,  -- a delete has no "after"
        event:after.name::string                       as name,
        event:after.email::string                      as email,
        event:after.created_at::timestamp_tz           as created_at,
        event:op::string                               as op,
        event:source.lsn::bigint                       as lsn
    from {{ source('raw', 'customers') }}
),

latest as (
    select * from parsed
    qualify row_number() over (partition by id order by lsn desc) = 1
)

select id, name, email, created_at
from latest
where op != 'd'  -- filter deletes after picking the latest event, not before
