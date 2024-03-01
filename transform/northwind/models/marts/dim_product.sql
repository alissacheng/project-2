with categories as (
    select
        category_id,
        category_name,
        description
    from {{ ref('categories') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    discontinued,
    c.category_name,
    c.description as category_description
from {{ ref('products') }}
join {{ ref('categories') }} as c
    on c.category_id = products.category_id