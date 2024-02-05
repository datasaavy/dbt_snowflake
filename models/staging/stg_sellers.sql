with sellers as (
    select seller_id, seller_city, seller_zip_code_prefix, seller_state
    from {{ source("ecomm", "sellers") }}
)

select *
from sellers
limit 10
