-- Customer Demographics Incremental Model
-- This dbt model represents an incremental view for the 'customer_demographics' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['customer_type_id']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - customer_type_id: The identifier for each customer type.
-- - customer_desc: The description of the customer type.
-- - last_update: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'customer_demographics'.
-- Adjust the unique key and incremental strategy based on your specific requirements.

{{
    config(
        materialized="incremental",
        unique_key=["customer_type_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    customer_type_id,
    customer_desc,
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'customer_demographics') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}