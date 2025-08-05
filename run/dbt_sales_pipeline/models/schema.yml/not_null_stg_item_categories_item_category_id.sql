select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from SALES_PIPELINE.RAW_staging.stg_item_categories
where item_category_id is null



      
    ) dbt_internal_test