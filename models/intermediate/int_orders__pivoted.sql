{{ config(
    materialized='table',
    partition_by={
      "field": "order_id",
      "data_type": "int64",
      "range": {
         "start": 0,
         "end": 10000,
         "interval": 100
      }
    },
    dataset = 'jaffle_shop'
)}}

{#
   https://docs.getdbt.com/reference/resource-configs/bigquery-configs#use-project-and-dataset-in-configurations
#}


{%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}

with payments as (
   select * from {{ ref('stg_payments') }}
),

final as (
   select
       order_id,
       {% for payment_method in payment_methods -%}

       sum(case when payment_method = '{{ payment_method }}' then amount else 0 end)
            as {{ payment_method }}_amount

       {%- if not loop.last -%}
         ,
       {% endif -%}

       {%- endfor %}
   from {{ ref('stg_payments') }}
   group by 1
)

select * from final