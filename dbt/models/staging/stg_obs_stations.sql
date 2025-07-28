{{ config(
    materialized = "view",
    unique_key   = "station_id"
) }}

with raw as (
    select
      id                         as station_id,
      name                       as station_name,
      country                    as country_code,
      region                     as region_code,
      wmo                        as wmo_id,
      icao                       as icao_code,
      latitude,
      longitude,
      elevation,
      timezone,
      cast(hourly_start as date)  as hourly_start_date,
      cast(hourly_end as date)    as hourly_end_date,
      cast(daily_start as date)   as daily_start_date,
      cast(daily_end as date)     as daily_end_date,
      cast(monthly_start as date) as monthly_start_date,
      cast(monthly_end as date)   as monthly_end_date,
      snapshot_time               as snapshot_time
    from {{ source('landing', 'land_obs_stations') }}
),

latest_snapshot as (
    select max(snapshot_time) as snapshot_time
    from raw
),

final as (
    select *
    from raw
    where snapshot_time = (select snapshot_time from latest_snapshot)
)

select * from final
