select 
    customer_id,
    o.order_id,
    amount
from {{ ref('stg_payments') }} as p
inner join {{ ref('stg_orders') }} as o
on p.order_id = o.order_id
