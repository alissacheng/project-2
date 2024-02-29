{{
    config(
        materialized="table",
        unique_key=["territory_id"]
    )
}}

select
REGION_ID,
TERRITORY_ID,
TERRITORY_DESCRIPTION
    
from {{ source('northwind', 'territories') }}
