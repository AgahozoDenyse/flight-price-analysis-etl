select
    source,
    destination,
    count(*)                  as booking_count,
    round(avg(total_fare), 2) as avg_total_fare
from {{ ref('stg_flight_prices') }}
group by source, destination
order by booking_count desc
