-- models/staging/stg_shops.sql

with source as (
    select * FROM {{ source('raw', 'translated_shops') }}
),

renamed as (
    select
        shop_id::int as shop_id,
        shop_name::string as shop_name
    from source
)

select * from renamed


