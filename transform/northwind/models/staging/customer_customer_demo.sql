-- Customer Customer Demo Incremental Model
-- This dbt model represents an incremental view for the 'customer_customer_demo' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['customer_id']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - customer_id: The unique identifier for each customer.
-- - customer_type_id: The identifier for the customer type.
-- - last_update: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'customer_customer_demo'.
-- Adjust the unique key and incremental strategy based on your specific requirements.

{{
    config(
        materialized="incremental",
        unique_key=["customer_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    customer_id,
    customer_type_id,
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'customer_customer_demo') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}