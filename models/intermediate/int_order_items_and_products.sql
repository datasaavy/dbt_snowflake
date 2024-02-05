order_items_and_products as (
    select 
        ot.order_id as order_id,
        ot.product_id as product_id,
        p.product_category_name as product_category_name

    from
    order_items ot
    LEFT JOIN
    products p
    USING(product_id)

)

select * from order_items_and_products