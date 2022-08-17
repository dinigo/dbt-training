{%- set payment_methods = ['bank_transfer', 'cupon', 'credit_card', 'gift_card', 'bitcoin'] -%}

with

payments as (
    select * from {{ ref('stg_payments') }} 
),
pivoted as (
    select
        order_id,
        {%- for payment_method in payment_methods %}
        sum(case when payment_method = "{{payment_method}}" then amount else 0 end) as {{payment_method}}_amount
        {%- if not loop.last -%},{% endif %}
        {%- endfor %}
     from payments
     group by order_id
)
select * from pivoted