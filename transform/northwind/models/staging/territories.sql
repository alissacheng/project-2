-- Territories Incremental Model
-- This dbt model represents an incremental view for the 'territories' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['TERRITORY_ID']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - REGION_ID: The identifier for the region to which the territory belongs.
-- - TERRITORY_ID: The unique identifier for each territory.
-- - TERRITORY_DESCRIPTION: The description of the territory.
-- - LAST_UPDATE: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'territories'.
-- Adjust the unique key and incremental strategy based on your specific requirements.


{{
    config(
        materialized="incremental",
        unique_key=["territory_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    REGION_ID,
    TERRITORY_ID,
    TERRITORY_DESCRIPTION,
    _airbyte_extracted_at as LAST_UPDATE
    
from {{ source('northwind', 'territories') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}