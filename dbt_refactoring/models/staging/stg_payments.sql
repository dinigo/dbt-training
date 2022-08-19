select
    id as payment_id,
    'success' as payment_status,
    order_id,
    round(amount/100.0,2) as payment_amount,
    date('2022-01-01') as created
from {{ ref('payments') }}
