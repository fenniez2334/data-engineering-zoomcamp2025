{{ config(materialized='table') }}

with quarterly_revenue as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        concat(extract(year from pickup_datetime), '/Q', extract(quarter from pickup_datetime)) as year_quarter,
        sum(total_amount) as total_revenue
    from {{ ref('fact_trips') }}
    group by 1, 2, 3, 4
),

yoy_growth as (
    select
        curr.service_type,
        curr.year_quarter,
        curr.total_revenue as current_revenue,
        prev.total_revenue as previous_revenue,
        round(
            ((curr.total_revenue - prev.total_revenue) / prev.total_revenue) * 100, 2
        ) as yoy_growth_percent
    from quarterly_revenue curr
    left join quarterly_revenue prev
        on curr.service_type = prev.service_type
        and curr.year = prev.year + 1
        and curr.quarter = prev.quarter
)

select *
from yoy_growth
order by service_type, year_quarter