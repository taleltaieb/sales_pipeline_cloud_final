
    
    

select
    shop_id as unique_field,
    count(*) as n_records

from SALES_PIPELINE.RAW_staging.stg_shops
where shop_id is not null
group by shop_id
having count(*) > 1


