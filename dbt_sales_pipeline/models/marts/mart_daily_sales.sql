with sales as (

    select
        date,
        shop_id,
        item_id,
        sum(item_cnt_day) as total_item_cnt,
        sum(item_price * item_cnt_day) as total_revenue

    from {{ ref('stg_sales_train') }}
    group by date, shop_id, item_id

)

select *
from sales
