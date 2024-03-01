select
    employee_id
from warehouse_northwind.staging.orders
where employee_id > 9 and employee_id < 1
