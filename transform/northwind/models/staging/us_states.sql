-- US States Incremental Model
-- This dbt model represents an incremental view for the 'us_states' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['STATE_ID']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - STATE_ID: The unique identifier for each US state.
-- - STATE_ABBR: The abbreviation of the state.
-- - STATE_NAME: The full name of the state.
-- - STATE_REGION: The region to which the state belongs.
-- - LAST_UPDATE: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'us_states'.
-- Adjust the unique key and incremental strategy based on your specific requirements.


{{
    config(
        materialized="incremental",
        unique_key=["state_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    STATE_ID,
    STATE_ABBR,
    STATE_NAME,
    STATE_REGION,
    _airbyte_extracted_at as LAST_UPDATE
    
from {{ source('northwind', 'us_states') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}