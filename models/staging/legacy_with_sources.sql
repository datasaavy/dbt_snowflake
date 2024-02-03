with 
customers as (

    select * from {{source('ecomm','customers')}}
   
),
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
orders as (
    select * from {{source('ecomm','orders')}}
),

geo_location as (
    select * from {{source('ecomm','geo_location')}}
   ),

order_items as (
    select * from {{source('ecomm','order_items')}}
    
),

payments as (
    select * from {{source('ecomm','order_payments')}}
    
),

order_ratings as (
    select * from {{source('ecomm','order_review_ratings')}}
    
),

products as (
    select * from {{source('ecomm','products')}}
),

sellers as (
    select * from {{source('ecomm','sellers')}}
   LIMIT 10 
)

select * from sellers