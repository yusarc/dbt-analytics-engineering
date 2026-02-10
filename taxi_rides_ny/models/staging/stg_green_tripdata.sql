select 
   --identifiers
   cast(VendorID as int) as vendor_id,
   cast(RatecodeID as int) as rate_code_id,
   cast(PULocationID as int) as pickup_location_id,
   cast(DOLocationID as int) as dropoff_location_id,

   --timestamps
   cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
   cast(lpep_dropoff_datetime as timestamp) dropoff_datetime,
   
   --trip info
   cast(passenger_count as int) as passenger_count,
   cast(trip_distance as float) as trip_distance,
   cast(trip_type as int) as trip_type,
   
   --payment info
   cast(fare_amount as numeric) as fare_amount,
   cast(extra as numeric) as extra,
   cast(mta_tax as numeric) as mta_tax,
   cast(tip_amount as numeric) as tip_amount,
   cast(tolls_amount as numeric) as tolls_amount,
   cast(improvement_surcharge as numeric) as improvement_surcharge,
   cast(total_amount as numeric) as total_amount,
   cast(payment_type as int) as payment_type

from {{source('raw_data', 'green_tripdata')}}
where vendor_id is not null;