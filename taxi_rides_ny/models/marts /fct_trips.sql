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
--delete duplicated rows
deduped_trips as (
    select *
    from (
        select
            t.*,
            row_number() over (
                partition by
                    vendor_id,
                    rate_code_id,
                    pickup_location_id,
                    dropoff_location_id,
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
                    payment_type
                order by pickup_datetime
            ) as rn
        from trips_with_pk t
    )
    where rn = 1  
)

select
    trip_id,
    vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
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
from trips_enriched

