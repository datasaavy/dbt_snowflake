with 
order_and_order_items as (
    select * from {{ ref('int_order_and_order_items') }}
),

order_items_and_products as (
    select * from {{ ref('int_order_items_and_products') }}
),

product_sales as (
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

select * from product_sales
