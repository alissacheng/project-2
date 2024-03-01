with orders as (
    select
        ship_via,
        order_id,
        freight
    from {{ ref('orders') }}
),

order_details as (
    select
        order_id,
        unit_price,
        quantity,
        discount,
        (unit_price - discount) * quantity  as revenue
    from {{ ref('order_details') }}
),

shippers as (
    select
        shipper_id,
        company_name,
        phone
    from {{ ref('shippers') }}
)

select
    distinct
    {{ dbt_utils.generate_surrogate_key(['shipper_id']) }} as ship_key,
    shipper_id,
    company_name,
    phone,
    count(orders.order_id) over (partition by shipper_id) as total_orders_shipped,
    sum(orders.freight) over (partition by shipper_id) as total_freight_costs,
    sum(od.revenue) over (partition by shipper_id) as total_revenue_shipped,
    round((total_freight_costs / total_revenue_shipped) *  100, 2) || '%' as percent_sales_spent_on_freight
from shippers
left join orders as orders
    on orders.ship_via = shippers.shipper_id
left join order_details as od
    on orders.order_id = od.order_id
order by shipper_id
