select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from SALES_PIPELINE.RAW_marts.mart_revenue_by_month
where date_block_num is null



      
    ) dbt_internal_test