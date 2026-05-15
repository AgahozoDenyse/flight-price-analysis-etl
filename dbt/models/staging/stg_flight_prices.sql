with source as (
    select * from {{ source('raw', 'flight_price_raw') }}
),

cleaned as (
    select
        lower(trim(airline))                                            as airline,
        lower(trim(source))                                             as source,
        lower(trim(source_name))                                        as source_name,
        lower(trim(destination))                                        as destination,
        lower(trim(destination_name))                                   as destination_name,
        lower(trim(departure_date_and_time))                            as departure_date_and_time,
        lower(trim(arrival_date_and_time))                              as arrival_date_and_time,
        lower(trim(duration_hrs))                                       as duration_hrs,
        lower(trim(stopovers))                                          as stopovers,
        lower(trim(aircraft_type))                                      as aircraft_type,
        lower(trim("class"))                                            as booking_class,
        lower(trim(booking_source))                                     as booking_source,
        lower(trim(seasonality))                                        as seasonality,
        cast(days_before_departure as numeric)                          as days_before_departure,
        cast(base_fare as numeric)                                      as base_fare,
        cast(tax_and_surcharge as numeric)                              as tax_and_surcharge,
        cast(base_fare as numeric) + cast(tax_and_surcharge as numeric) as total_fare
    from source
    where
        airline           is not null and trim(airline)       != ''
        and source        is not null and trim(source)        != ''
        and destination   is not null and trim(destination)   != ''
        and base_fare     is not null
        and tax_and_surcharge is not null
        and cast(base_fare as numeric)         >= 0
        and cast(tax_and_surcharge as numeric) >= 0
)

select * from cleaned
