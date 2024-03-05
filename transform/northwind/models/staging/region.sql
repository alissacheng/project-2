-- Region Incremental Model
-- This dbt model represents an incremental view for the 'region' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['region_id']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - region_id: The unique identifier for each region.
-- - region_description: The description of the region.
-- - last_update: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'region'.
-- Adjust the unique key and incremental strategy based on your specific requirements.


{{
    config(
        materialized="incremental",
        unique_key=["region_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    REGION_ID,
    REGION_DESCRIPTION,
    _airbyte_extracted_at as LAST_UPDATE
    
from {{ source('northwind', 'region') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
