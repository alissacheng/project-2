select
    {{ dbt_utils.generate_surrogate_key(["date.month_end_date", "product_key"]) }} as sale_monthly_key,
    date.month_end_date as order_month_end_date,
    product_key,
    sum(revenue) as revenue
from {{ ref('fact_order') }} as fa
inner join {{ ref('dim_date') }} as date
    on fa.order_date = date.date_day
group by
    date.month_end_date,
    product_key