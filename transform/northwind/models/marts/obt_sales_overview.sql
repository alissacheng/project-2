-- Manual One Big Table using staging tables
-- surrogate keys on order, customer, product, supplier, and category ids

select
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['od.product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['p.supplier_id']) }} as supplier_key, 
    {{ dbt_utils.generate_surrogate_key(['p.category_id']) }} as category_key,
    o.freight,
    o.ship_city,
    o.ship_name,
    o.order_date,
    cu.company_name as customer_company,
    o.ship_address,
    o.ship_country,
    o.ship_postal_code,
    o.shipped_date,
    od.unit_price,
    od.quantity,
    od.discount,
    p.product_name,
    p.quantity_per_unit,
    c.category_name,
    c.description,
    s.company_name as supplier_name,
    cu.company_name as customer_name

--ordered by order date for efficient micro partitioning
from {{ ref('orders') }} as o
    join {{ ref('order_details') }} as od 
        on o.order_id = od.order_id
    join {{ ('products') }} as p 
        on p.product_id = od.product_id
    join {{ ('categories') }} as c on 
        c.category_id = p.category_id
    join {{ ('suppliers') }} as s on 
        s.supplier_id = p.supplier_id
    join {{ ('customers') }} as cu on 
        cu.customer_id = o.customer_id
order by o.order_date