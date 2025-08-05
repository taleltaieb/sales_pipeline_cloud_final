
  create or replace  view SALES_PIPELINE.RAW_staging.stg_items 
  
   as (
    -- models/staging/stg_items.sql

with source as (
    select * FROM SALES_PIPELINE.RAW.translated_items
),

renamed as (
    select
        item_id::int as item_id,
        item_name::string as item_name,
        item_category_id::int as item_category_id
    from source
)

select * from renamed
  );
