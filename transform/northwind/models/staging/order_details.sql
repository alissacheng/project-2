{{
    config(
        materialized="incremental",
        unique_key=["order_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    order_id,
    discount,
    quantity,
    product_id,
    unit_price,
    cast(_airbyte_extracted_at as datetime) as last_update
    
from {{ source('northwind', 'order_details') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}