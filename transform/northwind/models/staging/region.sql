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
    cast(_airbyte_extracted_at as datetime) as last_update
    
from {{ source('northwind', 'region') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
