-- incremental approach with delete + insert

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
    cast(_airbyte_extracted_at as datetime) as last_update
    
from {{ source('northwind', 'territories') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}