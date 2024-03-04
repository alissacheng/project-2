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
