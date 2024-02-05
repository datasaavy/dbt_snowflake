with order_ratings as (
        select review_id, order_id, review_score
        from {{ source("ecomm", "order_review_ratings") }}
    )
SELECT * FROM order_ratings
LIMIT 10 

    