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