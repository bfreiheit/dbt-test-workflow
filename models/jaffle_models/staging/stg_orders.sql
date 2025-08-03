select    
    customer as customer_id,   
    pr.field_type, 
    count(distinct store_id) as cnt_stores,
    sum(subtotal) as sum_subtotal,
    sum(price) as sum_price,
    sum(sp.cost) as sum_cost,
    count(od.id) as cnt_orders
from {{ source("raw", "raw_orders") }} od
join {{ source('raw', 'raw_items')}} it on it.order_id = od.id
join {{ source('raw', 'raw_products')}} pr on pr.sku = it.sku
join {{ source('raw', 'raw_supplies')}} sp on sp.sku = it.sku
where ordered_at >= datetime '2018-12-01'  -- nutzt Partition Pruning
and ordered_at <= datetime '2019-04-01'
group by 1, 2
