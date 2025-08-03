select
    customer as customer_id,
    min(date(ordered_at)) as first_order_date,
    max(date(ordered_at)) as most_recent_order_date,
    count(distinct od.id) as number_of_orders,
    sum(subtotal) as sum_subtotal,
    count(tw.id) as cnt_tweets
from {{ source("raw", "raw_orders") }} od
left join {{ source('raw', 'raw_tweets')}} tw on tw.user_id = od.customer
where ordered_at >= datetime '2018-12-01'  -- nutzt Partition Pruning
and ordered_at <= datetime '2019-04-01'
group by 1