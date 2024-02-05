/* Tables List
customers
orders
geo_location
order_items
order_payments
order_review_ratings
products
sellers
*/
with
    customers as (
        select
            customer_id,
            customer_city,
            customer_state,
            customer_zip_code_prefix,
            customer_unique_id
        from {{ source("ecomm", "customers") }}
    ),

    orders as (
        select order_id, order_status, order_purchase_timestamp, customer_id
        from {{ source("ecomm", "orders") }}
    ),
/*
    order_id, order_purchase_timestamp, customer_id from orders
    customer_id, customer_city,customer_state from customers
    order_id, product_id, seller_id, price from order_items
    order_id, payment_type, payment_value from payments
    order_id, review_score from order_ratings
    product_id, product_category_name from products
    seller_id, seller_city, seller_zip_code_prefix, seller_state from sellers

order_id, customer_id, cutomer_city, customer_state, product_id, product_category, price, payment_type, payment_value, order_rating, 

TABLE____1

orders_custom as
(
select 
    o.order_id, o.order_purchase_timestamp as ordered_date_ts, o.customer_id,
    c.customer_city, c.customer_state
from orders o
LEFT JOIN
customers c
USING(customer_id)
)

select * from orders_custom

TABLE____2
order_id, order_purchase_timestamp, customer_id from orders
order_id, product_id, seller_id, price from order_items
order_id, payment_type, payment_value from payments

orders_payments as (

select 
    o.order_id, o.order_purchase_timestamp,
    p.payment_type, p.payment_value,
    ot.product_id, ot.price

from orders o
LEFT JOIN
payments p
USING(order_id)
)

select * from orders_payments

TABLE___3
    order_id, product_id, seller_id, price from order_items
    product_id, product_category_name from products

order_prod_info as (
    select
    ot.order_id, p.product_id, p.product_category_name, ot.price
    from 
    products p
    LEFT JOIN
    order_items ot
    USING(product_id)

)
select * from order_prod_info



TABLE_4 = TABLE_1_&_TABLE_3

final_alpha as (

    select
        oc.order_id, oc.ordered_date_ts,
        opi.order_id, opi.product_id, opi.product_category_name, opi.price,
        oc.customer_city, oc.customer_state

    from
    orders_custom oc
    LEFT JOIN
    order_prod_info opi
    USING(order_id)

)
select * from final_alpha

TABLE_5= TABLE_4 & TABLE_2

final_table as (
    select         
        fp.opi.order_id, fp.product_id, fp.product_category_name, fp.price,
        op.payment_type, op.payment_value,
        fp.customer_city, fp.customer_state

    from
    final_alpha fp
    LEFT JOIN
    orders_payments op

)


*/
    geo_location as (
        select
            geolocation_zip_code_prefix as zip_code,
            geolocation_city as city,
            geolocation_lat,
            geolocation_state as state,
            geolocation_lng
        from {{ source("ecomm", "geo_location") }}
    ),

    order_items as (
        select order_id, order_item_id, product_id, seller_id, price
        from {{ source("ecomm", "order_items") }}
    ),
/*

*/
    payments as (
        select order_id, payment_type, payment_value
        from {{ source("ecomm", "order_payments") }}
    ),

    order_ratings as (
        select review_id, order_id, review_score
        from {{ source("ecomm", "order_review_ratings") }}
    ),

    products as (
        select product_id, product_category_name from {{ source("ecomm", "products") }}
    ),

    sellers as (
        select seller_id, seller_city, seller_zip_code_prefix, seller_state
        from {{ source("ecomm", "sellers") }}
        limit 10
    ),

/*
TABLE____1
*/
orders_custom as
(
select 
    o.order_id, o.order_purchase_timestamp as ordered_date_ts, o.customer_id,
    c.customer_city, c.customer_state
from orders o
LEFT JOIN
customers c
USING(customer_id)
),
/*
select * from orders_custom
*/

/*TABLE____2
order_id, order_purchase_timestamp, customer_id from orders
order_id, product_id, seller_id, price from order_items
order_id, payment_type, payment_value from payments
*/
orders_payments as (

select 
    o.order_id, o.order_purchase_timestamp,
    p.payment_type, p.payment_value
    

from orders o
LEFT JOIN
payments p
USING(order_id)
),
/*
select * from orders_payments
*/
/*
TABLE___3
    order_id, product_id, seller_id, price from order_items
    product_id, product_category_name from products
*/
order_prod_info as (
    select
    ot.order_id, p.product_id, p.product_category_name, ot.price
    from 
    products p
    LEFT JOIN
    order_items ot
    USING(product_id)

),

/*
select * from order_prod_info
*/

/*
TABLE_4 = TABLE_1_&_TABLE_3
*/
final_alpha as (

    select
        oc.order_id, oc.ordered_date_ts,
        opi.order_id, opi.product_id, opi.product_category_name, opi.price,
        oc.customer_city, oc.customer_state

    from
    orders_custom oc
    LEFT JOIN
    order_prod_info opi
    USING(order_id)

),

/*
select * from final_alpha
*/

/*
TABLE_5= TABLE_4 & TABLE_2
*/
final_table_beta as (
    select         
        fp.order_id, fp.product_category_name, fp.price,
        op.payment_type, op.payment_value,
        fp.customer_city, fp.customer_state

    from
    final_alpha fp
    LEFT JOIN
    orders_payments op
    USING(order_id)

)

select * from final_table_beta
LIMIT 10

/**********Testing*************/

