-- Products Incremental Model
-- This dbt model represents an incremental view for the 'products' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['product_id']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - product_id: The unique identifier for each product.
-- - product_name: The name of the product.
-- - unit_price: The price per unit of the product.
-- - category_id: The unique identifier for each product category.
-- - supplier_id: The unique identifier for each product supplier.
-- - discontinued: Indicates whether the product has been discontinued.
-- - reorder_level: The reorder level for the product.
-- - units_in_stock: The quantity of units currently in stock.
-- - units_on_order: The quantity of units currently on order.
-- - quantity_per_unit: The quantity of product per unit (e.g., per case or per bottle).
-- - last_update: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'products'.
-- Adjust the unique key and incremental strategy based on your specific requirements.


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
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'products') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}