-- incremental approach with delete + insert

{{
    config(
        materialized="incremental",
        unique_key=["order_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    order_id,
    freight,
    ship_via,
    ship_city,
    ship_name,
    cast(order_date as date) as order_date,
    customer_id,
    employee_id,
    ship_region,
    ship_address,
    ship_country,
    cast(shipped_date as date) as shipped_date,
    required_date,
    ship_postal_code,
    cast(_airbyte_extracted_at as datetime) as last_update
    
from {{ source('northwind', 'orders') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}