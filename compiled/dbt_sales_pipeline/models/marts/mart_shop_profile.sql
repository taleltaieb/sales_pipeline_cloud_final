WITH monthly_shop_revenue AS (
    SELECT
        shop_id,
        DATE_TRUNC('month', TO_DATE(date, 'DD.MM.YYYY')) AS month,
        SUM(item_price * item_cnt_day) AS monthly_revenue
    FROM SALES_PIPELINE.RAW_staging.stg_sales_train
    GROUP BY shop_id, month
),

shop_stats AS (
    SELECT
        s.shop_id,
        COUNT(DISTINCT i.item_category_id) AS category_count,
        AVG(s.item_price) AS avg_item_price,
        SUM(s.item_price * s.item_cnt_day) / COUNT(DISTINCT s.item_id) AS revenue_per_item
    FROM SALES_PIPELINE.RAW_staging.stg_sales_train s
    LEFT JOIN SALES_PIPELINE.RAW_staging.stg_items i ON s.item_id = i.item_id
    GROUP BY s.shop_id
),

revenue_variance AS (
    SELECT
        shop_id,
        STDDEV(monthly_revenue) AS revenue_stddev
    FROM monthly_shop_revenue
    GROUP BY shop_id
),

final AS (
    SELECT
        ss.shop_id,
        sc.shop_name,
        ss.category_count,
        ROUND(ss.avg_item_price, 2) AS avg_item_price,
        ROUND(ss.revenue_per_item, 2) AS revenue_per_item,
        ROUND(rv.revenue_stddev, 2) AS monthly_revenue_variance
    FROM shop_stats ss
    LEFT JOIN SALES_PIPELINE.RAW_staging.stg_shops sc ON ss.shop_id = sc.shop_id
    LEFT JOIN revenue_variance rv ON ss.shop_id = rv.shop_id
    ORDER BY revenue_per_item DESC
    LIMIT 10
)

SELECT * FROM final