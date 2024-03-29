--CTE for Orders and Order details 
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

--Pulling a distinct list of orders and details using surrogate key for customer id
--utilizes window function for counting orders per customer
--utilizes window function for sum of revenue per customer
--AS utilized to alias fields
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
    count(o.order_id) over (partition by o.customer_id) as total_orders_made,
    sum(od.revenue) over (partition by o.customer_id) as total_dollars_spent

from {{ ref('customers') }}
join orders as o
    on o.customer_id = customers.customer_id
join order_details as od
    on od.order_id = o.order_id