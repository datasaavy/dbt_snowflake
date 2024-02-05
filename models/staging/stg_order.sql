with orders as (select * from {{ source("ecomm", "orders") }}),

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
)

select * from order_extract

