with 
customers as (

    select * from RAW_DATABASE.SQL_ECOM_DBO.customers
   
),

orders as (
    select * from RAW_DATABASE.SQL_ECOM_DBO.ORDERS
),

geo_location as (
    select * from RAW_DATABASE.SQL_ECOM_DBO.geo_location
   ),

order_items as (
    select * from RAW_DATABASE.SQL_ECOM_DBO.order_items
    
),

payments as (
    select * from RAW_DATABASE.SQL_ECOM_DBO.order_payments
    
),

order_ratings as (
    select * from RAW_DATABASE.SQL_ECOM_DBO.order_review_ratings
    
),

products as (
    select * from RAW_DATABASE.SQL_ECOM_DBO.products
    
),

sellers as (
    select * from RAW_DATABASE.SQL_ECOM_DBO.sellers
    
)

select * from sellers