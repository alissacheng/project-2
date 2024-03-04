{{
    config(
        materialized="incremental",
        unique_key=["EMPLOYEE_ID", "TERRITORY_ID"],
        incremental_strategy="delete+insert"
    )
}}

select
EMPLOYEE_ID,
TERRITORY_ID,
_airbyte_extracted_at as LAST_UPDATE
    
from {{ source('northwind', 'employee_territories') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}