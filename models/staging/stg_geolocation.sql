with geo_location as (
        select
            geolocation_zip_code_prefix as zip_code,
            geolocation_city as city,
            geolocation_lat,
            geolocation_state as state,
            geolocation_lng
        from {{ source("ecomm", "geo_location") }}
    )

select * from geo_location
LIMIT 10