WITH shop_revenue AS (
    SELECT
        s.shop_id,
        shp.shop_name,
        ROUND(SUM(s.item_price * s.item_cnt_day), 2) AS total_revenue
    FROM {{ ref('stg_sales_train') }} s
    LEFT JOIN {{ ref('stg_shops') }} shp
        ON s.shop_id = shp.shop_id
    GROUP BY s.shop_id, shp.shop_name
)

SELECT
    shop_id,
    shop_name,
    total_revenue
FROM shop_revenue
ORDER BY total_revenue DESC
