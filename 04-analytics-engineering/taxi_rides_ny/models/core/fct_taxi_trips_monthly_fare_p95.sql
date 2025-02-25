{{ config(materialized='table') }}

with valid_trips as (
    select
        service_type,
        extract(year from pickup_datetime) as pickup_year,
        extract(month from pickup_datetime) as pickup_month,
        fare_amount
    from {{ ref('fact_trips') }}
    where
        fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit card')
),

percentiles as (
    select
        service_type,
        pickup_year,
        pickup_month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p90
    from valid_trips
)

SELECT
    service_type,
    pickup_year,
    pickup_month,
    MAX(p97) AS p97, -- Aggregate to get a single value per group
    MAX(p95) AS p95, -- Aggregate to get a single value per group
    MAX(p90) AS p90  -- Aggregate to get a single value per group
FROM percentiles
GROUP BY service_type, pickup_year, pickup_month
order by service_type, pickup_year, pickup_month