SELECT 
  item_id,
  SUM(item_cnt_day) AS total_units_sold
FROM RAW.sales_train
GROUP BY item_id
ORDER BY total_units_sold DESC
LIMIT 10