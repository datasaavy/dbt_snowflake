with orders as (select * from {{ source("ecomm", "orders") }}) 
select * from orders
LIMIT 10
