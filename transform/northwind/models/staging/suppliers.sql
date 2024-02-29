{{
    config(
        materialized="table",
        unique_key=["supplier_id"]
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
CONTACT_TITLE
    
from {{ source('northwind', 'suppliers') }}



