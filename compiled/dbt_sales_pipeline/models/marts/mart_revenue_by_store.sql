SELECT 
  shop_id,
  SUM(item_price * item_cnt_day) AS total_revenue
FROM RAW.sales_train
GROUP BY shop_id