{{ config(materialized='table') }}


fhv_data as (
    select *
    from {{ ref('stg_fvh_rides') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
## keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
select 
    fhv_data.dispatching_base_num, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_data.pickup_datetime, 
    fhv_data.dropoff_datetime, 
    fhv_data.affiliated_base_number
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid