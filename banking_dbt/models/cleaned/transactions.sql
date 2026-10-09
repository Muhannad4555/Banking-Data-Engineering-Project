with parsed as (
    select
        coalesce(event:after.id, event:before.id)::int as id,
        event:after.account_id::int                    as account_id,
        event:after.type::string                       as type,
        event:after.amount::number(12,2)               as amount,
        event:after.created_at::timestamp_tz           as created_at,
        event:op::string                               as op,
        event:source.lsn::bigint                       as lsn
    from {{ source('raw', 'transactions') }}
),

latest as (
    select * from parsed
    qualify row_number() over (partition by id order by lsn desc) = 1
)

select id, account_id, type, amount, created_at
from latest
where op != 'd'
