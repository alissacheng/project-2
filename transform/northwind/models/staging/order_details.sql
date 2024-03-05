-- Order Details Incremental Model
-- This dbt model represents an incremental view for the 'order_details' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['order_id']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - order_id: The unique identifier for each order.
-- - discount: The discount applied to the order.
-- - quantity: The quantity of products ordered.
-- - product_id: The unique identifier for each product.
-- - unit_price: The price per unit of the product.
-- - last_update: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'order_details'.
-- Adjust the unique key and incremental strategy based on your specific requirements.

{{
    config(
        materialized="incremental",
        unique_key=["order_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    order_id,
    discount,
    quantity,
    product_id,
    unit_price,
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'order_details') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}