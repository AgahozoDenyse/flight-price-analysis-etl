select
    airline,
    round(avg(base_fare), 2)         as avg_base_fare,
    round(avg(tax_and_surcharge), 2) as avg_tax_and_surcharge,
    round(avg(total_fare), 2)        as avg_total_fare
from {{ ref('stg_flight_prices') }}
group by airline
order by avg_total_fare desc
