select
    seasonality,
    round(avg(total_fare), 2) as avg_total_fare,
    round(avg(base_fare), 2)  as avg_base_fare,
    count(*)                  as booking_count
from {{ ref('stg_flight_prices') }}
group by seasonality
order by avg_total_fare desc
