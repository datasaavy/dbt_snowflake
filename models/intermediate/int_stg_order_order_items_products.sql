{{
  config(
    materialized='view'
  )
}}
with orders as (select * from {{ source("ecomm", "orders") }}),

order_items as (select order_id, order_item_id, product_id, seller_id, price from {{ source("ecomm", "order_items") }}),

products as (select product_id, product_category_name from {{ source("ecomm", "products") }} ),

order_extract as (
    select 
    order_id, 
    order_status, 
    order_purchase_timestamp, 
    customer_id,
    to_date(order_purchase_timestamp) as order_date,
    to_time(order_purchase_timestamp) as order_time,
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month,
    extract(year from order_date) as order_year,
    case
        when extract(month from order_date) = 1 then 'January'
        when extract(month from order_date) = 2 then 'February'
        when extract(month from order_date) = 3 then 'March'
        
        when extract(month from order_date) = 4 then 'April'
        when extract(month from order_date) = 5 then 'May'
        when extract(month from order_date) = 6 then 'June'
        
        when extract(month from order_date) = 7 then 'July'
        when extract(month from order_date) = 8 then 'August'
        when extract(month from order_date) = 9 then 'September'
        
        when extract(month from order_date) = 10 then 'October'
        when extract(month from order_date) = 11 then 'November'
        when extract(month from order_date) = 12 then 'December'
        -- ... (add remaining months)
        else 'Invalid month'
    end as order_month_name

    from orders
),
/************order and order_items(ref:order_id)*********************/
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
),

/************order_items and products(product_id)*********************/
order_items_and_products as (
    select 
        ot.order_id as order_id,
        ot.product_id as product_id,
        p.product_category_name as product_category_name

    from
    order_items ot
    LEFT JOIN
    products p
    USING(product_id)

),

/******(order_and_order_items)**and**(order_items_and_products)**using ref(order_id)*****/
final_tab as (
    select
        oot.order_id,
        oot.order_date,
        oot.order_year,
        oot.order_month_name,
        otp.product_id,
        otp.product_category_name,
        oot.price
    from
    order_and_order_items oot
    LEFT JOIN
    order_items_and_products otp
    USING(order_id)
    
)

select * from final_tab
LIMIT 10