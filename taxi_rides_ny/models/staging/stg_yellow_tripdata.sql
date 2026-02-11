select
-- identifiers (standardized naming for consistency across yellow/green)
    cast(VendorID as integer) as vendor_id,
    cast(RatecodeID as integer) as rate_code_id,
    cast(PULocationID as integer) as pickup_location_id,
    cast(DOLocationID as integer) as dropoff_location_id,

-- timestamps (standardized naming)
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime, -- tpep = Taxicab Passanger
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

--  trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    1 as trip_type, --yellow taxis can only be street-hail (trip_type=1)

-- payment info
    cast (fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast (tolls_amount as numeric) as tolls_amount,
    0 as ehail_fee, --yellow taxis do not have ehail fees
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type
    
from {{source('raw_data','yellow_tripdata')}}

-- filter out records with null vendor_id (data quality requirement)

where vendor_id is not null