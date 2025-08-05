WITH base_date_cte AS (
    SELECT MIN(TO_DATE(DATE, 'DD.MM.YYYY')) AS base_date
    FROM SALES_PIPELINE.STAGING_STAGING.stg_sales_train
), sales_data AS (
    SELECT
        s.date_block_num,
        TO_CHAR(DATEADD(month, s.date_block_num, b.base_date), 'YYYY-MM') AS date_month,
        SUM(s.item_cnt_day * s.item_price) AS monthly_revenue
    FROM SALES_PIPELINE.RAW_staging.stg_sales_train s
    CROSS JOIN base_date_cte b
    GROUP BY s.date_block_num, b.base_date
)

SELECT
    date_block_num,
    date_month,
    monthly_revenue
FROM sales_data
ORDER BY date_block_num