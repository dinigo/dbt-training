select
    {{ dbt_utils.surrogate_key(['customer_id', 'order_id']) }} as id,
    customer_id,
    order_id,
from {{ ref('fct_orders') }}
