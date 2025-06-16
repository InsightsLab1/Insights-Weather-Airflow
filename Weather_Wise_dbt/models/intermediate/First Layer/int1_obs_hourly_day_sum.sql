{{ config(
    materialized = "view",
    unique_key   = "station_date_id"
) }}

with hourly as (
    select
      station_id,
      cast(obs_time as date) as obs_date,
      temp_c,
      temp_f,
      dwpt_c,
      dwpt_f,
      rhum_pct,
      precipitation_mm,
      snow_mm,
      wind_dir_degrees,
      wind_speed_km_h,
      wind_peak_gust_km_h,
      air_pressure_hpa,
      sunshine_minutes
    from {{ ref('stg_obs_hourly') }}
),

aggregated as (
    select
      station_id,
      obs_date,
      -- temperature
      avg(temp_c) as avg_temp_c,
      avg(temp_f) as avg_temp_f,
      min(temp_c) as min_temp_c,
      max(temp_c) as max_temp_c,
      -- dew point
      avg(dwpt_c) as avg_dwpt_c,
      avg(dwpt_f) as avg_dwpt_f,
      -- humidity & pressure
      avg(rhum_pct)        as avg_rhum_pct,
      avg(air_pressure_hpa) as avg_air_pressure_hpa,
      -- wind
      avg(wind_dir_degrees)     as avg_wind_dir_degrees,
      avg(wind_speed_km_h)      as avg_wind_speed_km_h,
      max(wind_peak_gust_km_h)  as max_wind_peak_gust_km_h,
      -- precipitation & snow
      sum(precipitation_mm) as total_precip_mm,
      sum(snow_mm)          as total_snow_mm,
      sum(sunshine_minutes) as total_sunshine_minutes
    from hourly
    group by station_id, obs_date
),

final as (
    select
      {{ dbt_utils.generate_surrogate_key(['station_id', 'obs_date']) }} as station_date_id,
      *
    from aggregated
)

select * from final
