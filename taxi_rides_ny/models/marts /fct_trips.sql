with trips_unioned as (

    select *
    from {{ ref('int_trips_unioned') }}

),

-- add primary key
trips_with_pk as (

    select
        md5(
            concat(
                cast(vendor_id as string), ':',
                cast(pickup_datetime as string), ':',
                cast(dropoff_datetime as string), ':',
                cast(pickup_location_id as string), ':',
                cast(dropoff_location_id as string)
            )
        ) as trip_id,
        *
    from trips_unioned

),

-- add payment_type description
trips_enriched as (

    select
        t.*,
        case cast(t.payment_type as int)
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            else 'Other'
        end as payment_type_description
    from trips_with_pk t

),

-- join zones to get pickup/dropoff zone names
zones as (

    select *
    from {{ ref('dim_zones') }}

),

trips_with_zones as (

    select
        te.*,
        p.zone as pickup_zone,
        d.zone as dropoff_zone
    from trips_enriched te
    left join zones p
        on te.pickup_location_id = p.location_id
    left join zones d
        on te.dropoff_location_id = d.location_id

)

select
    trip_id,
    vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_zone,
    dropoff_zone,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    payment_type_description
from trips_with_zones

