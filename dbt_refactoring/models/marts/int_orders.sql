with

orders as (
  select * from {{ ref('stg_orders') }}
),
payments as (
  select * from {{ ref('stg_payments') }}
  where  payment_status not in ('fail', 'pending')
),
-- logic transformations

order_totals as (
  select
    order_id,
    payment_status,
    sum(payment_amount) as order_value_dollars
  from payments
  group by order_id, payment_status
),

order_values_join as (
  select
    orders.*,
    order_totals.payment_status,
    order_totals.order_value_dollars
  from orders
  left join order_totals using (order_id)
)

select * from order_values_join