{{ config(
    materialized = "table",
    unique_key   = "station_id"
) }}

with raw as (
    select * 
    from {{ ref('stg_obs_stations') }}
),

final as (
    select
        station_id,
        station_name,
        country_code,
        region_code,
        wmo_id,
        icao_code,
        latitude,
        longitude,
        elevation,
        timezone,
        hourly_start_date,
        hourly_end_date,
        daily_start_date,
        daily_end_date,
        monthly_start_date,
        monthly_end_date,
        snapshot_time
    from raw
)

select * from final
