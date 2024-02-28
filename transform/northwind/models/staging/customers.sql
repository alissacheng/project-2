{{
    config(
        materialized="table",
        unique_key=["customer_id"]
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
    contact_title
    
from {{ source('northwind', 'customers') }}
