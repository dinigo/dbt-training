select
    id as payment_id,
    'success' as payment_status,
    order_id,
    round(amount/100.0,2) as order_value_dollars,
from {{ ref('payments') }}
