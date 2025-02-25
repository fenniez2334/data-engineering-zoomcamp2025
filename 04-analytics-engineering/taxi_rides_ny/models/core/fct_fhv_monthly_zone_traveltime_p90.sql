{{ config(materialized='table') }}

WITH trip_data AS (
    SELECT 
        trip_year,
        trip_month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        trip_duration
    FROM {{ ref('dim_fhv_trips') }}
    WHERE pickup_zone is not null and dropoff_zone is not null
),

pct_data AS (
    SELECT 
        trip_year,
        trip_month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (
            PARTITION BY trip_year, trip_month, pickup_locationid, dropoff_locationid
        ) AS p90_trip_duration
    FROM trip_data
),

ranked_dropoff_zones AS (
    SELECT 
        trip_year,
        trip_month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        p90_trip_duration,
        DENSE_RANK() OVER (
            PARTITION BY trip_year, trip_month, pickup_locationid
            ORDER BY p90_trip_duration DESC
        ) AS duration_rank
    FROM pct_data
)


SELECT 
    trip_year,
    trip_month,
    pickup_locationid,
    dropoff_locationid,
    pickup_zone,
    dropoff_zone,
    p90_trip_duration,
    duration_rank
FROM ranked_dropoff_zones
ORDER BY 
    trip_year, 
    trip_month, 
    pickup_locationid, 
    dropoff_locationid