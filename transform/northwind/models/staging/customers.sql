-- Customers Incremental Model
-- This dbt model represents an incremental view for the 'customers' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['customer_id']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - customer_id: The unique identifier for each customer.
-- - fax: The fax number of the customer.
-- - city: The city where the customer is located.
-- - phone: The phone number of the customer.
-- - region: The region where the customer is located.
-- - address: The address of the customer.
-- - country: The country where the customer is located.
-- - postal_code: The postal code of the customer.
-- - company_name: The name of the customer's company.
-- - contact_name: The name of the contact person at the customer's company.
-- - contact_title: The title of the contact person at the customer's company.
-- - last_update: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'customers'.
-- Adjust the unique key and incremental strategy based on your specific requirements.

{{
    config(
        materialized="incremental",
        unique_key=["customer_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    customer_id,
    fax,
    city,
    phone,
    region,
    address,
    country,
    postal_code,
    company_name,
    contact_name,
    contact_title,
    _airbyte_extracted_at as last_update
    
from {{ source('northwind', 'customers') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}