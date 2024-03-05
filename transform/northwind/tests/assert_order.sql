-- Select quantity and unit_price from warehouse_northwind.staging.order_details
-- where both quantity and unit_price are less than 0.
select
    quantity,
    unit_price
from warehouse_northwind.staging.order_details
where quantity < 0 and unit_price < 0
