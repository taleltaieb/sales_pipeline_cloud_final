

      create or replace transient table SALES_PIPELINE.RAW_marts.mart_revenue_by_store  as
      (SELECT 
  shop_id,
  SUM(item_price * item_cnt_day) AS total_revenue
FROM RAW.sales_train
GROUP BY shop_id
      );
    