WITH sales_data AS (
    SELECT
        date,
        item_price,
        item_cnt_day,
        item_price * item_cnt_day AS revenue
    FROM SALES_PIPELINE.RAW_staging.stg_sales_train
)

SELECT
    SUM(revenue) AS total_revenue
FROM sales_data