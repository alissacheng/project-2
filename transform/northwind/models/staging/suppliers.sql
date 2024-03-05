-- Shippers Incremental Model
-- This dbt model represents an incremental view for the 'shippers' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['SHIPPER_ID']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - PHONE: The phone number of the shipper.
-- - SHIPPER_ID: The unique identifier for each shipper.
-- - COMPANY_NAME: The name of the shipper.
-- - last_update: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'shippers'.
-- Adjust the unique key and incremental strategy based on your specific requirements.


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

