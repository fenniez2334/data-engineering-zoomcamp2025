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
)

SELECT *
FROM trip_data
ORDER BY 
    trip_year, 
    trip_month, 
    pickup_locationid, 
    dropoff_locationid