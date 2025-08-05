select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from SALES_PIPELINE.RAW_marts.mart_shop_profile
where SHOP_NAME is null



      
    ) dbt_internal_test