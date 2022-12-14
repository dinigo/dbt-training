with
    customers as (
        select * from {{ ref('stg_customers') }}
    ),
    orders as (
        select * from {{ ref('stg_orders') }}
    ),
    employes as (
        select * from {{ ref('stg_employees') }}
    ),
    customer_orders as (
        select
            customer_id,
            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(orders.order_id) as number_of_orders,
            sum(amount) as lifetime_value
        from orders 
        left join {{ ref('fct_orders') }} using (customer_id)
        group by customer_id
    ),

    final as (
        select
            customers.customer_id,
            customers.first_name,
            customers.last_name,
            customer_orders.first_order_date,
            customer_orders.most_recent_order_date,
            employes.employee_id is not null as is_employee,
            customer_orders.lifetime_value,
            coalesce(customer_orders.number_of_orders, 0) as number_of_orders

        from customers
        left join customer_orders using (customer_id)
        left join employes using (customer_id)
    )

select * from final