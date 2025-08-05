
    
    

select
    item_category_id as unique_field,
    count(*) as n_records

from SALES_PIPELINE.RAW_staging.stg_item_categories
where item_category_id is not null
group by item_category_id
having count(*) > 1


