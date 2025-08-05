WITH base_date_cte AS (
    SELECT MIN(TO_DATE(date, 'DD.MM.YYYY')) AS base_date
    FROM SALES_PIPELINE.RAW_staging.stg_sales_train
),

monthly_sales AS (
    SELECT
        DATE_BLOCK_NUM,
        ITEM_ID,
        SUM(ITEM_CNT_DAY * ITEM_PRICE) AS MONTHLY_REVENUE
    FROM SALES_PIPELINE.RAW_staging.stg_sales_train
    GROUP BY DATE_BLOCK_NUM, ITEM_ID
),

item_total_revenue AS (
    SELECT
        ITEM_ID,
        SUM(MONTHLY_REVENUE) AS TOTAL_REVENUE
    FROM monthly_sales
    GROUP BY ITEM_ID
),

ranked_items AS (
    SELECT
        ITEM_ID,
        TOTAL_REVENUE,
        RANK() OVER (ORDER BY TOTAL_REVENUE DESC) AS REVENUE_RANK
    FROM item_total_revenue
),

filtered_top_items AS (
    SELECT ITEM_ID
    FROM ranked_items
    WHERE REVENUE_RANK <= 30
),

final_matrix AS (
    SELECT
        m.DATE_BLOCK_NUM,
        m.ITEM_ID,
        i.ITEM_NAME,
        m.MONTHLY_REVENUE,
        TO_CHAR(DATEADD(month, m.DATE_BLOCK_NUM, b.base_date), 'Mon YYYY') AS MONTH_LABEL,
        DATEADD(month, m.DATE_BLOCK_NUM, b.base_date) AS MONTH_DATE
    FROM monthly_sales m
    JOIN filtered_top_items t ON m.ITEM_ID = t.ITEM_ID
    LEFT JOIN SALES_PIPELINE.RAW_staging.stg_items i ON m.ITEM_ID = i.ITEM_ID
    CROSS JOIN base_date_cte b
)

SELECT *
FROM final_matrix