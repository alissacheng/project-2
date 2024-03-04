{{
    config(
        materialized="incremental",
        unique_key=["customer_type_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    customer_type_id,
    customer_desc,
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'customer_demographics') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}