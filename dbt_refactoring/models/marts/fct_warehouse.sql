with

payments as (
    select * from {{ ref('stg_payments') }}
    where payment_status <> 'fail'
),
orders as (
        select * from {{ ref('stg_orders') }}
),
customers as (
        select * from {{ ref('stg_customers') }}
),

order_cost as (
    select
        order_id,
        max(created) as payment_finalized_date,
        sum(payment_amount) / 100.0 as total_amount_paid
    from payments
    group by order_id
),

paid_orders as (
    select orders.id as order_id,
        orders.user_id    as customer_id,
        orders.order_date as order_placed_at,
        orders.order_status,
        order_cost.total_amount_paid,
        order_cost.payment_finalized_date,
        customers.first_name    as customer_first_name,
            customers.last_name as customer_last_name
    from orders
    left join  order_cost on orders.id = order_cost.order_id
    left join customers on orders.user_id = customers.id
),

paid_by_customer as (
    select
        order_id,
        sum(total_amount_paid) as clv_bad
    from paid_orders
    group by order_id, customer_id
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders
    from customers
    left join orders using(customer_id)
    group by customer_id
),

final as (
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
        case
            when customer_orders.first_order_date = paid_orders.order_placed_at
            then 'new'
            else 'return'
        end as nvsr,

        paid_by_customer.clv_bad as customer_lifetime_value,
        customer_orders.first_order_date as fdos
    from paid_orders
    left join customer_orders using (customer_id)
    left outer join paid_by_customer using(order_id)
)

select * from final
