with
    customers as (select * from raw_database.sql_ecom_dbo.customers),
    /*
customers
orders
geo_location
order_items
order_payments
order_review_ratings
products
sellers
*/
    orders as (select * from raw_database.sql_ecom_dbo.orders),

    geo_location as (select * from raw_database.sql_ecom_dbo.geo_location),

    order_items as (select * from raw_database.sql_ecom_dbo.order_items),

    payments as (select * from raw_database.sql_ecom_dbo.order_payments),

    order_ratings as (select * from raw_database.sql_ecom_dbo.order_review_ratings),

    products as (select * from raw_database.sql_ecom_dbo.products),

    sellers as (select * from raw_database.sql_ecom_dbo.sellers limit 10)

select *
from sellers
