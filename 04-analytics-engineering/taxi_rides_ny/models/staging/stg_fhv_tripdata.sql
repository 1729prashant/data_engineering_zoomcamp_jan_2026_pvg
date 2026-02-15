with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        999 as vendor_id,
        0 as rate_code_id,
        cast(PULocationID as integer) as pickup_location_id,
        cast(DOLocationID as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,  -- lpep = Licensed Passenger Enhancement Program (green taxis)
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        ' ' as store_and_fwd_flag,
        0 as passenger_count,
        0 as trip_distance,
        0 as trip_type,

        -- payment info
        0 as fare_amount,
        0 as extra,
        0 as mta_tax,
        0 as tip_amount,
        0 as tolls_amount,
        0 as ehail_fee,
        0 as improvement_surcharge,
        0 as total_amount,
        0 as payment_type
        
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}