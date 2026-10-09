with parsed as (
    select
        coalesce(event:after.id, event:before.id)::int as id,
        event:after.customer_id::int                   as customer_id,
        event:after.balance::number(12,2)              as balance,
        event:after.created_at::timestamp_tz           as created_at,
        event:op::string                               as op,
        event:source.lsn::bigint                       as lsn
    from {{ source('raw', 'accounts') }}
),

latest as (
    select * from parsed
    qualify row_number() over (partition by id order by lsn desc) = 1
)

select id, customer_id, balance, created_at
from latest
where op != 'd'
