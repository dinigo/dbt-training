select
    id as payment_id,
    order_id,
    payment_method,
    {{ cents_to_dollars('amount', 3) }} as amount
from {{ ref('raw_payments') }}
