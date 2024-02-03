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

    geo_location as (
        select
            geolocation_zip_code_prefix,
            geolocation_city,
            geolocation_lat,
            geolocation_state,
            geolocation_lng
        from {{ source("ecomm", "geo_location") }}
    ),

    order_items as (
        select order_id, order_item_id, product_id, seller_id, price
        from {{ source("ecomm", "order_items") }}
    ),

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

    

select *
from sellers
