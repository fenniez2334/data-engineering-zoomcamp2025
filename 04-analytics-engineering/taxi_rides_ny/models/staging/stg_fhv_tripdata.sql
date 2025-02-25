{{
    config(
        materialized='view'
    )
}}

select
    dispatching_base_num,
    PUlocationID as pickup_locationid,
    DOlocationID as dropoff_locationid,
    dropoff_datetime, 
    pickup_datetime, 
    SR_Flag,
    Affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where dispatching_base_num is not null



-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}