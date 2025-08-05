select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from SALES_PIPELINE.RAW_marts.mart_shop_profile
where REVENUE_PER_ITEM is null



      
    ) dbt_internal_test