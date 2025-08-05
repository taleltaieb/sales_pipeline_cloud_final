{% macro date_month_from_block_num_dynamic(date_block_num_col) %}
    TO_CHAR(DATEADD(
        month,
        {{ date_block_num_col }},
        (SELECT BASE_DATE FROM {{ ref('base_date') }})
    ), 'YYYY-MM')
{% endmacro %}

