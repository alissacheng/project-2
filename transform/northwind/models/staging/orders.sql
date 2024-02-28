{{
    config(
        materialized="table",
        unique_key=["order_id"]
    )
}}

select
    order_id,
    freight,
    ship_via,
    ship_city,
    ship_name,
    order_date,
    customer_id,
    employee_id,
    ship_region,
    ship_address,
    ship_country,
    shipped_date,
    required_date,
    ship_postal_code
    
from {{ source('northwind', 'orders') }}