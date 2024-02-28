{{
    config(
        materialized="table",
        unique_key=["order_id"]
    )
}}

select
    order_id,
    discount,
    quantity,
    product_id,
    unit_price
    
from {{ source('northwind', 'order_details') }}
