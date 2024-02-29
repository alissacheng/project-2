{{
    config(
        materialized="table",
        unique_key=["EMPLOYEE_ID", "TERRITORY_ID"]
    )
}}

select
EMPLOYEE_ID,
TERRITORY_ID
    
from {{ source('northwind', 'employee_territories') }}
