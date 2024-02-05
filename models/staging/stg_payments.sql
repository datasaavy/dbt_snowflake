with payments as (
        select order_id, payment_type, payment_value
        from {{ source("ecomm", "order_payments") }}
    )

select * from payments

LIMIT 10

    