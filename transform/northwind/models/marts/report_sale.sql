select
    {{ dbt_utils.star(from=ref('fact_sale'), relation_alias='fact_sale', except=[
        "product_key", "customer_key", "shipper_key", "date_day"
    ]) }},
    {{ dbt_utils.star(from=ref('dim_product'), relation_alias='dim_product', except=["product_key"]) }},
    {{ dbt_utils.star(from=ref('dim_customer'), relation_alias='dim_customer', except=["customer_key"]) }},
    {{ dbt_utils.star(from=ref('dim_shipper'), relation_alias='dim_shipper', except=["shipper_key"]) }},
    {{ dbt_utils.star(from=ref('dim_date'), relation_alias='dim_date', except=["date_day"]) }}
from {{ ref('fact_sale') }} as fact_sale
left join {{ ref('dim_product') }} as dim_product on fact_sale.product_key = dim_product.product_key
left join {{ ref('dim_customer') }} as dim_customer on fact_sale.customer_key = dim_customer.customer_key
left join {{ ref('dim_shipper') }} as dim_shipper on fact_sale.shipper_key = dim_shipper.shipper_key
left join {{ ref('dim_date') }} as dim_date on fact_sale.order_date = dim_date.date_day