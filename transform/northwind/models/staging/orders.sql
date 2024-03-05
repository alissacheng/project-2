-- Orders Incremental Model
-- This dbt model represents an incremental view for the 'orders' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['order_id']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - order_id: The unique identifier for each order.
-- - freight: The freight cost associated with the order.
-- - ship_via: The method of shipment for the order.
-- - ship_city: The city to which the order will be shipped.
-- - ship_name: The name of the entity to which the order will be shipped.
-- - order_date: The date when the order was placed.
-- - customer_id: The unique identifier for each customer.
-- - employee_id: The unique identifier for each employee associated with the order.
-- - ship_region: The region to which the order will be shipped.
-- - ship_address: The address to which the order will be shipped.
-- - ship_country: The country to which the order will be shipped.
-- - shipped_date: The date when the order was shipped.
-- - required_date: The date by which the order is required.
-- - ship_postal_code: The postal code of the shipping address.
-- - last_update: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'orders'.
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
    freight,
    ship_via,
    ship_city,
    ship_name,
    order_date,
    customer_id,
    employee_id,
    ship_region,
    ship_address,
    ship_country,
    shipped_date,
    required_date,
    ship_postal_code,
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'orders') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}