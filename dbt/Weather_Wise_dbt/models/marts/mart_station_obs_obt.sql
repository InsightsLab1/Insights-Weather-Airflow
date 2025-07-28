{{ config(
    materialized = "table",
    unique_key   = "station_date_id"
) }}

with

raw_obs as (
  select * from {{ ref('int2_obs_daily_all') }}
),

stations as (
  select * from {{ ref('stg_obs_stations') }}
),

final as (
  select
    obs.station_date_id,
    obs.station_id,
    s.station_name,
    s.country_code,
    s.region_code,
    s.wmo_id,
    s.icao_code,
    s.latitude,           
    s.longitude,
    s.elevation,
    s.timezone,
    s.hourly_start_date,
    s.hourly_end_date,
    s.daily_start_date,
    s.daily_end_date,
    s.monthly_start_date,
    s.monthly_end_date,
    obs.obs_date,
    obs.tavg_f,
    obs.tmin_f,
    obs.tmax_f,
    obs.avg_dwpt_f,
    obs.avg_rhum_pct,
    obs.min_rhum_pct,
    obs.max_rhum_pct,
    obs.precipitation_mm as precip_mm,
    obs.snow_mm          as snow_mm,
    obs.wind_dir_degrees as wind_dir_deg,
    obs.wind_speed_km_h  as wind_speed_kmh,
    obs.wind_peak_gust_km_h as wind_gust_kmh,
    obs.air_pressure_hpa as pressure_hpa,
    obs.sunshine_minutes as sunshine_min
  from raw_obs obs
  left join stations s using(station_id)
)

select * from final
