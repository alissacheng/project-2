with orders as (
    select
        order_id,
        customer_id
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

select distinct
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} as customer_key,
    o.customer_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    region,
    postal_code,
    country,
    phone,
    fax,
    count(o.order_id) over (partition by o.customer_id) as total_orders,
    sum(od.revenue) over (partition by o.customer_id) as total_purchased

from {{ ref('customers') }}
join orders as o
    on orders.customer_id = customers.customer_id
join order_details as od
    on od.order_id = orders.order_id