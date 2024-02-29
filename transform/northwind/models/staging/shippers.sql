{{
    config(
        materialized="table",
        unique_key=["SHIPPER_ID"]
    )
}}

select
PHONE,
SHIPPER_ID,
COMPANY_NAME
    
from {{ source('northwind', 'shippers') }}
