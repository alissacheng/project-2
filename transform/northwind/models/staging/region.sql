{{
    config(
        materialized="table",
        unique_key=["region_id"]
    )
}}

select
REGION_ID,
REGION_DESCRIPTION
    
from {{ source('northwind', 'region') }}


