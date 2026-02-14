with source as (

    select 
        *
    from {{ source('raw_data', 'fhv_tripdata_2019') }}

),

renamed as (

    select 
        -- identifiers
        cast(dispatching_base_num as varchar) as dispatching_base_num,
        cast(PUlocationID as int)            as pickup_location_id,
        cast(DOlocationID as int)            as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp)   as pickup_datetime,
        cast(dropOff_datetime as timestamp)  as dropoff_datetime,

        -- service type
        'FHV'                                as service_type

    from source
    where PUlocationID is not null
      and DOlocationID is not null
)

select * from renamed
