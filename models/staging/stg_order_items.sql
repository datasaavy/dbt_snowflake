with order_items as (
        select order_id, order_item_id, product_id, seller_id, price
        from {{ source("ecomm", "order_items") }}
    )
select * from order_items
