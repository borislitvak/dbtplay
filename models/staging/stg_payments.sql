select  
    orderid as order_id, 
    paymentmethod as payment_method,
    amount
from {{ source('stripe', 'payment')}}
