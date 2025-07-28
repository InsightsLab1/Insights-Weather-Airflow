{{ config(
    materialized = "table",
    unique_key   = "station_id"
) }}

with raw as (
    select *
    from {{ ref('stg_obs_stations') }}
),

-- Count distincts of observations
distincts as (
    select
        station_id,
        count(distinct sunshine_minutes) as distinct_sunshine_minutes_count,
        count(distinct tmax_f) as distinct_tmax_f_count,
        sum( case when tmax_f is not null then 1 else 0 end ) as tmax_f_count
    from {{ ref('int2_obs_daily_all') }}
    group by station_id
),

final as (
    select
        r.station_id,
        r.station_name,
        r.country_code,
        r.region_code,
        r.region_name,
        r.wmo_id,
        r.icao_code,
        r.latitude,
        r.longitude,
        r.elevation,
        r.timezone,
        r.hourly_start_date,
        r.hourly_end_date,
        r.daily_start_date,
        r.daily_end_date,
        r.monthly_start_date,
        r.monthly_end_date,
        r.snapshot_time,
        ds.distinct_sunshine_minutes_count,
        ds.distinct_tmax_f_count,
        ds.tmax_f_count

    from raw r
    left join distincts ds
      on r.station_id = ds.station_id
)

select * from final
