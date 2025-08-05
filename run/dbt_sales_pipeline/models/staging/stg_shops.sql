
  create or replace  view SALES_PIPELINE.RAW_staging.stg_shops 
  
   as (
    -- models/staging/stg_shops.sql

with source as (
    select * FROM SALES_PIPELINE.RAW.translated_shops
),

renamed as (
    select
        shop_id::int as shop_id,
        shop_name::string as shop_name
    from source
)

select * from renamed
  );
