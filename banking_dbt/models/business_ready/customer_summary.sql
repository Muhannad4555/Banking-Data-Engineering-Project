-- One row per customer: current balance and lifetime deposits/withdrawals.
-- Balances and transactions are summed in separate CTEs so joining them does not double count.
with balances as (
    select customer_id, sum(balance) as total_balance
    from {{ ref('accounts') }}
    group by 1
),

activity as (
    select
        a.customer_id,
        count(*)                                         as transactions,
        sum(iff(t.type = 'deposit', t.amount, 0))        as total_deposits,
        sum(iff(t.type = 'withdrawal', t.amount, 0))     as total_withdrawals
    from {{ ref('transactions') }} t
    join {{ ref('accounts') }} a on a.id = t.account_id
    group by 1
)

select
    c.id as customer_id,
    c.name,
    c.email,
    coalesce(b.total_balance, 0)      as total_balance,
    coalesce(x.transactions, 0)       as transactions,
    coalesce(x.total_deposits, 0)     as total_deposits,
    coalesce(x.total_withdrawals, 0)  as total_withdrawals
from {{ ref('customers') }} c
left join balances b on b.customer_id = c.id
left join activity x on x.customer_id = c.id
