with customers as (
    select * from {{ ref('stg_customers')}}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
), 
customer_orders_payments as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(o.order_id) as number_of_orders,
        sum(amount) as lifetime_value
    from orders o
    inner join payments p
    on o.order_id = p.order_id
    group by 1
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        first_order_date,
        most_recent_order_date,
        coalesce(number_of_orders, 0) as number_of_orders,
        lifetime_value 
    from customers
    left join customer_orders_payments using (customer_id)
)
select * from final