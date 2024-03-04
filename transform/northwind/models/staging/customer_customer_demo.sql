{{
    config(
        materialized="incremental",
        unique_key=["customer_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    customer_id,
    customer_type_id,
    cast(_airbyte_extracted_at as datetime) as last_update
    
from {{ source('northwind', 'customer_customer_demo') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}