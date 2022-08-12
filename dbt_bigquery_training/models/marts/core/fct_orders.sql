select
    order_id,
    customer_id,
    sum(amount) as amount
from {{ ref('stg_orders') }}
left join {{ ref('stg_payments') }} using (order_id)
group by order_id, customer_id