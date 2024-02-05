with order_extract as   (
    select * from {{ ref('stg_order') }}
),

order_items as  (
    select * from {{ ref('stg_order_items') }}
),

order_and_order_items as (
select 
    oe.order_id as order_id,
    oe.order_date as order_date,
    oe.order_year as order_year,
    oe.order_month_name as order_month_name,
    ot.product_id as product_id,
    ot.price as price
from
order_extract oe
LEFT JOIN
order_items ot
USING(order_id) 
)

select * from order_and_order_items
