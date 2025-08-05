WITH base_date_cte AS (
    SELECT MIN(TO_DATE(DATE, 'DD.MM.YYYY')) AS base_date
    FROM SALES_PIPELINE.STAGING_STAGING.stg_sales_train
), base AS (
    SELECT
        s.date_block_num,
        TO_CHAR(DATEADD(month, s.date_block_num, b.base_date), 'YYYY-MM') AS date_month,
        s.shop_id,
        s.item_id,
        SUM(s.item_cnt_day * s.item_price) AS monthly_revenue,
        SUM(s.item_cnt_day) AS monthly_qty_sold
    FROM SALES_PIPELINE.STAGING_STAGING.stg_sales_train s
    CROSS JOIN base_date_cte b
    GROUP BY s.date_block_num, s.shop_id, s.item_id, b.base_date
)

SELECT *
FROM base
ORDER BY date_block_num, shop_id, item_id

