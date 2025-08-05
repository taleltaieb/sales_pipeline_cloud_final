
  create or replace  view SALES_PIPELINE.RAW_staging.base_date 
  
   as (
    SELECT
  MIN(DATE) AS BASE_DATE
FROM SALES_PIPELINE.RAW_staging.stg_sales_train
  );
