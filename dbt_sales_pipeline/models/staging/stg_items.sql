-- models/staging/stg_items.sql

with source as (
    select * FROM {{ source('raw', 'translated_items') }}
),

renamed as (
    select
        item_id::int as item_id,
        item_name::string as item_name,
        item_category_id::int as item_category_id
    from source
)

select * from renamed


