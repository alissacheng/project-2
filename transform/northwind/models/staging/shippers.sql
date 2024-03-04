{{
    config(
        materialized="incremental",
        unique_key=["SHIPPER_ID"],
        incremental_strategy="delete+insert"
    )
}}

select
    PHONE,
    SHIPPER_ID,
    COMPANY_NAME,
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'shippers') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}