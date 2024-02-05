with products as (
        select product_id, product_category_name from {{ source("ecomm", "products") }}
    )
SELECT * FROM products


