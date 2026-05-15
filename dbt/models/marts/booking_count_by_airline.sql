select
    airline,
    count(*) as booking_count
from {{ ref('stg_flight_prices') }}
group by airline
order by booking_count desc
