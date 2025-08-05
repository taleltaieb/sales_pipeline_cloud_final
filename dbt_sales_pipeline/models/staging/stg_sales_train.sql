-- models/staging/stg_sales_train.sql

with source as (
    select
        date,
        cast(date_block_num as int) as date_block_num,
        cast(shop_id as int) as shop_id,
        cast(item_id as int) as item_id,
        cast(item_price as float) as item_price,
        cast(item_cnt_day as float) as item_cnt_day
    FROM {{ source('raw', 'sales_train') }}
)

select * from source
