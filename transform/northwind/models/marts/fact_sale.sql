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

select
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['o.ship_via']) }} as ship_key,
    o.order_id,
    o.order_date,
    unit_price,
    quantity,
    discount,
    revenue
from order_details
inner join orders as o
    on o.order_id = order_details.order_id
