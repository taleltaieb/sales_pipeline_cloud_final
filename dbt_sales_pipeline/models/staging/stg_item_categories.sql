-- models/staging/stg_item_categories.sql

with source as (
    select * FROM {{ source('raw', 'translated_item_categories') }}
),

renamed as (
    select
        item_category_id::int as item_category_id,
        item_category_name::string as item_category_name
    from source
)

select * from renamed


