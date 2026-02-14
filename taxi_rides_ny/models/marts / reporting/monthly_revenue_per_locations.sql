
with trips_data as (

    select *
    from {{ ref('fct_trips') }}

),

monthly_revenue as (

    select
        pickup_location_id                  as revenue_zone_id,
        date_trunc('month', pickup_datetime) as revenue_month,
        sum(total_amount)                   as monthly_revenue
    from trips_data
    group by
        revenue_zone_id,
        revenue_month

)

select *
from monthly_revenue