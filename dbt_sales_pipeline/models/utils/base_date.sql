SELECT
  MIN(DATE) AS BASE_DATE
FROM {{ ref('stg_sales_train') }}
