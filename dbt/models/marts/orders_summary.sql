with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    o.order_id,
    o.order_ref,
    o.amount,
    o.order_status,
    c.customer_name,
    c.customer_status,
    p.product_name,
    p.price
from orders          o
left join customers  c on o.order_id = c.customer_id
left join products   p on o.order_id = p.product_id
