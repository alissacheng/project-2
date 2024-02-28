{{
    config(
        materialized="table",
        unique_key=["category_id"]
    )
}}

select
    category_id,
    category_name,
    picture,
    description
    
from {{ source('northwind', 'categories') }}
