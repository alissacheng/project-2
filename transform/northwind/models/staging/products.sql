-- incremental approach with delete + insert

{{
    config(
        materialized="incremental",
        unique_key=["product_id"],
        incremental_strategy="delete+insert"
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
    quantity_per_unit,
    cast(_airbyte_extracted_at as datetime) as last_update
    
from {{ source('northwind', 'products') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}