-- incremental approach with delete + insert

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
    cast(_airbyte_extracted_at as datetime) as last_update
    
from {{ source('northwind', 'us_states') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}