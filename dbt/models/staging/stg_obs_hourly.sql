{{ config(
    materialized = "view",
    unique_key   = "station_time_id"
) }}

with raw as (
    select
      time                             as obs_time,
      station_id,
      {{ c_to_f('temp') }}             as temp_f,
      {{ c_to_f('dwpt') }}             as dwpt_f,
      rhum                             as rhum_pct,
      prcp                             as precipitation_mm,
      snow                             as snow_mm,
      wdir                             as wind_dir_degrees,
      wspd                             as wind_speed_km_h,
      wpgt                             as wind_peak_gust_km_h,
      pres                             as air_pressure_hpa,
      tsun                             as sunshine_minutes,
      coco                             as weather_condition_code,
      {{ dbt_utils.generate_surrogate_key(['station_id', 'obs_time']) }} as station_time_id
    from {{ source('landing', 'land_obs_hourly') }}
),
final as (
    select *
    from raw
)

select * from final
