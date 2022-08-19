select
    id as payment_id,
    'success' as payment_status,
    order_id,
    round(amount/100.0,2) as payment_amount,
from {{ ref('payments') }}
