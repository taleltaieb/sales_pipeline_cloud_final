

      create or replace transient table SALES_PIPELINE.RAW_marts.mart_top_products  as
      (SELECT 
  item_id,
  SUM(item_cnt_day) AS total_units_sold
FROM RAW.sales_train
GROUP BY item_id
ORDER BY total_units_sold DESC
LIMIT 10
      );
    