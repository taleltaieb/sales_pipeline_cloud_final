

      create or replace transient table SALES_PIPELINE.RAW_marts.mart_seasonality_by_category  as
      (WITH base_date_cte AS (
    SELECT MIN(TO_DATE(date, 'DD.MM.YYYY')) AS base_date
    FROM SALES_PIPELINE.RAW_staging.stg_sales_train
),

monthly_sales_with_category AS (
    SELECT
        m.date_block_num,
        i.item_category_id,
        c.item_category_name,
        DATEADD(month, m.date_block_num, b.base_date) AS date_month,
        SUM(m.monthly_revenue) AS monthly_revenue
    FROM SALES_PIPELINE.RAW_marts.mart_monthly_sales m
    LEFT JOIN SALES_PIPELINE.RAW_staging.stg_items i ON m.item_id = i.item_id
    LEFT JOIN SALES_PIPELINE.RAW_staging.stg_item_categories c ON i.item_category_id = c.item_category_id
    CROSS JOIN base_date_cte b
    GROUP BY 1, 2, 3, 4
),

monthly_revenue AS (
    SELECT
        item_category_id,
        item_category_name,
        TO_CHAR(date_month, 'Mon') AS month_label,
        EXTRACT(MONTH FROM date_month) AS month_num,
        SUM(monthly_revenue) AS revenue
    FROM monthly_sales_with_category
    GROUP BY 1, 2, 3, 4
),

category_avg AS (
    SELECT
        item_category_id,
        AVG(revenue) AS avg_revenue
    FROM monthly_revenue
    GROUP BY 1
),

seasonality_index AS (
    SELECT
        m.item_category_id,
        m.item_category_name,
        m.month_label,
        m.month_num,
        ROUND(m.revenue / NULLIF(c.avg_revenue, 0), 2) AS seasonality_score
    FROM monthly_revenue m
    JOIN category_avg c USING(item_category_id)
)

SELECT *
FROM seasonality_index
ORDER BY item_category_name, month_num
      );
    