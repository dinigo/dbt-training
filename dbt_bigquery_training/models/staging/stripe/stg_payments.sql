select
    order_id,
    amount
from {{ ref('raw_payments') }}
