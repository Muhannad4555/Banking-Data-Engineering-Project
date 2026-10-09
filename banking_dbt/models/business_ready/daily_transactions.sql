select
    created_at::date as day,
    type,
    count(*)         as transactions,
    sum(amount)      as total_amount
from {{ ref('transactions') }}
group by 1, 2
