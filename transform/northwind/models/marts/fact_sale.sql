-- Order and Order Details Join
-- This dbt model joins the 'orders' and 'order_details' tables to create a consolidated view of order-related information.

-- Columns:
-- order_key: A surrogate key generated using 'dbt_utils.generate_surrogate_key' for order records.
-- product_key: A surrogate key generated using 'dbt_utils.generate_surrogate_key' for product records.
-- customer_key: A surrogate key generated using 'dbt_utils.generate_surrogate_key' for customer records.
-- shipper_key: A surrogate key generated using 'dbt_utils.generate_surrogate_key' for shipper records.
-- order_id: The unique identifier for each order.
-- order_date: The date when the order was placed.
-- unit_price: The price per unit of the product.
-- quantity: The quantity of products ordered.
-- discount: The discount applied to the order.
-- revenue: The total revenue generated from the order.

-- Usage:
-- Include this code in your dbt project to create a consolidated view of order-related information.
-- Adjust the columns selected based on your specific reporting requirements.

-- Define a CTE 'orders' to select relevant information from the 'orders' table
with orders as (
    select
        order_id,
        customer_id,
        order_date,
        required_date,
        shipped_date,
        ship_via
    from {{ ref('orders') }}
),

-- Define a CTE 'order_details' to calculate revenue based on 'order_details' table
order_details as (
    select
        order_id,
        product_id,
        unit_price,
        quantity,
        discount,
        (unit_price - discount) * quantity  as revenue
    from {{ ref('order_details') }}
)

-- Final SELECT statement combining information from 'orders' and 'order_details'
select
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['o.ship_via']) }} as shipper_key,
    o.order_id,
    o.order_date,
    unit_price,
    quantity,
    discount,
    revenue
from order_details
inner join orders as o
    on o.order_id = order_details.order_id
