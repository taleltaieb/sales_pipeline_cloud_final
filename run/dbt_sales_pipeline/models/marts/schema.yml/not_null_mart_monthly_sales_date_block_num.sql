select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from SALES_PIPELINE.RAW_marts.mart_monthly_sales
where date_block_num is null



      
    ) dbt_internal_test