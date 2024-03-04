{{
    config(
        materialized="incremental",
        unique_key=["supplier_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    FAX,
    CITY,
    PHONE,
    REGION,
    ADDRESS,
    COUNTRY,
    HOMEPAGE,
    POSTAL_CODE,
    SUPPLIER_ID,
    COMPANY_NAME,
    CONTACT_NAME,
    CONTACT_TITLE,
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'suppliers') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}

