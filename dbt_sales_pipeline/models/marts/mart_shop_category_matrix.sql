WITH shop_category_sales AS (
    SELECT
        s.shop_id,
        sh.shop_name,
        ic.item_category_id,
        ic.item_category_name,
        ROUND(SUM(s.item_price * s.item_cnt_day), 2) AS total_revenue
    FROM {{ ref('stg_sales_train') }} s
    LEFT JOIN {{ ref('stg_items') }} i
        ON s.item_id = i.item_id
    LEFT JOIN {{ ref('stg_item_categories') }} ic
        ON i.item_category_id = ic.item_category_id
    LEFT JOIN {{ ref('stg_shops') }} sh
        ON s.shop_id = sh.shop_id
    GROUP BY s.shop_id, sh.shop_name, ic.item_category_id, ic.item_category_name
)

SELECT *
FROM shop_category_sales
ORDER BY shop_id, total_revenue DESC
