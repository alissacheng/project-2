{{
    config(
        materialized="table",
        unique_key=["product_id"]
    )
}}

select
    product_id,
    product_name,
    unit_price,
    category_id,
    supplier_id,
    discontinued,
    reorder_level,
    units_in_stock,
    units_on_order,
    quantity_per_unit
    
from {{ source('northwind', 'products') }}