WITH category_revenue AS (
    SELECT
        ic.item_category_id,
        ic.item_category_name,
        ROUND(SUM(s.item_price * s.item_cnt_day), 2) AS total_revenue
    FROM {{ ref('stg_sales_train') }} s
    LEFT JOIN {{ ref('stg_items') }} i
        ON s.item_id = i.item_id
    LEFT JOIN {{ ref('stg_item_categories') }} ic
        ON i.item_category_id = ic.item_category_id
    GROUP BY ic.item_category_id, ic.item_category_name
)

SELECT
    item_category_id,
    item_category_name,
    total_revenue
FROM category_revenue
ORDER BY total_revenue DESC
