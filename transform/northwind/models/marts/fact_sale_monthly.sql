-- Monthly Sales Aggregation
-- This dbt model aggregates monthly sales revenue based on the fact_sale and dim_date tables.

-- Columns:
-- sale_monthly_key: A surrogate key generated using 'dbt_utils.generate_surrogate_key' for monthly sales records.
-- order_month_end_date: The month-end date associated with the sales data.
-- product_key: The surrogate key associated with the product.
-- revenue: The total sales revenue for the specified month and product.

-- Usage:
-- Include this code in your dbt project to generate a table with aggregated monthly sales data.
-- Adjust the columns selected based on your specific reporting requirements.
select
    {{ dbt_utils.generate_surrogate_key(["date.month_end_date", "product_key"]) }} as sale_monthly_key,
    date.month_end_date as order_month_end_date,
    product_key,
    sum(revenue) as revenue
from {{ ref('fact_sale') }} as fs
inner join {{ ref('dim_date') }} as date
    on fs.order_date = date.date_day
group by
    date.month_end_date,
    product_key