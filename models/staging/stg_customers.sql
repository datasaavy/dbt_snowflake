with customers as (select * from {{ source("ecomm", "customers") }}) 
select * from customers
LIMIT 10