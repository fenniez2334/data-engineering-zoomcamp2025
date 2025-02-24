{{ config(materialized='table') }}

with valid_trips as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from {{ ref('fact_trips') }}
    where
        fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit Card')
),

percentiles as (
    select
        service_type,
        year,
        month,
        PERCENTILE_CONT(0.97) OVER (PARTITION BY service_type, year, month) AS p97,
        PERCENTILE_CONT(0.95) OVER (PARTITION BY service_type, year, month) AS p95,
        PERCENTILE_CONT(0.90) OVER (PARTITION BY service_type, year, month) AS p90
    from valid_trips
   
)

select *
from percentiles
order by service_type, year, month