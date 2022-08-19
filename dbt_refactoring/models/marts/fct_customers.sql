with
customers as (
  select * from {{ ref('stg_customers') }}
),
orders as (
  select * from {{ ref('stg_orders') }}
),
payments as (
  select * from {{ ref('stg_payments') }}
),

-- marts
customer_order_history as (
    select
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,
        min(order_date) as first_order_date,
        min(case when orders.status not in ('returned','return_pending')
          then order_date end)
        as first_non_returned_order_date,
        max(case when orders.status not in ('returned','return_pending')
          then order_date end)
        as most_recent_non_returned_order_date,
        coalesce(max(user_order_seq),0) as order_count,
        coalesce(count(case when orders.status != 'returned' then 1 end),0) as non_returned_order_count,
        sum(case when orders.status not in ('returned','return_pending')
          then order_value_dollars else 0 end) 
        as total_lifetime_value,
        sum(case when orders.status not in ('returned','return_pending')
          then order_value_dollars else 0 end)/nullif(count(case when orders.status not in ('returned','return_pending') 
          then 1 end),0)
        as avg_non_returned_order_value,
        array_agg(distinct orders.id) as order_ids

    from orders
    join customers using(customer_id)
    left outer join payments using(order_id)
    where orders.status not in ('pending')
    group by customers.customer_id, customers.full_name, customers.surname, customers.givenname

),

final as (
  select
      orders.order_id,
      orders.customer_id,
      orders.order_status,
      customers.surname,
      customers.givenname
      first_order_date,
      order_count,
      total_lifetime_value,
      order_value_dollars,
      payment_status
  from orders

  join customers
  on orders.customer_id = customers.customer_id

  join customer_order_history
  on orders.customer_id = customer_order_history.customer_id

  left outer join payments
  on orders.id = payments.order_id
)

select * from final