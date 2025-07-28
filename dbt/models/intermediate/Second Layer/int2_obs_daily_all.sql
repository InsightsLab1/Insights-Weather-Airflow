{{ config(
    materialized = "view",
    unique_key   = "station_date_id"
) }}

with

daily as (
    select
      station_id,
      obs_date,
      tavg_f,
      tmin_f,
      tmax_f,
      precipitation_mm,
      snow_mm,
      wind_dir_degrees,
      wind_speed_km_h,
      wind_peak_gust_km_h,
      air_pressure_hpa,
      sunshine_minutes
    from {{ ref('stg_obs_daily') }}
),

hourly_sum as (
    select
      station_id,
      obs_date,
      tavg_f      as hs_tavg_f,
      tmin_f      as hs_tmin_f,
      tmax_f      as hs_tmax_f,
      avg_dwpt_f,
      min_dwpt_f,
      max_dwpt_f,
      avg_rhum_pct,
      min_rhum_pct,
      max_rhum_pct,
      avg_air_pressure_hpa,
      avg_wind_dir_degrees,
      avg_wind_speed_km_h,
      max_wind_peak_gust_km_h,
      total_precip_mm,
      total_snow_mm,
      total_sunshine_minutes
    from {{ ref('int1_obs_hourly_day_sum') }}
),

-- daily rows + any matching hourly
left_part as (
    select
      coalesce(d.station_id, hs.station_id) as station_id,
      coalesce(d.obs_date, hs.obs_date)     as obs_date,

      coalesce(d.tavg_f, hs.hs_tavg_f)      as tavg_f,
      coalesce(d.tmin_f, hs.hs_tmin_f)      as tmin_f,
      coalesce(d.tmax_f, hs.hs_tmax_f)      as tmax_f,
      hs.avg_dwpt_f,
      hs.min_dwpt_f,
      hs.max_dwpt_f,
      hs.avg_rhum_pct,
      hs.min_rhum_pct,
      hs.max_rhum_pct,
      coalesce(d.precipitation_mm, hs.total_precip_mm)            as precipitation_mm,
      coalesce(d.snow_mm, hs.total_snow_mm)                       as snow_mm,
      coalesce(d.wind_dir_degrees, hs.avg_wind_dir_degrees)      as wind_dir_degrees,
      coalesce(d.wind_speed_km_h, hs.avg_wind_speed_km_h)        as wind_speed_km_h,
      coalesce(d.wind_peak_gust_km_h, hs.max_wind_peak_gust_km_h)as wind_peak_gust_km_h,
      coalesce(d.air_pressure_hpa, hs.avg_air_pressure_hpa)      as air_pressure_hpa,
      coalesce(d.sunshine_minutes, hs.total_sunshine_minutes)    as sunshine_minutes
    from daily d
    left join hourly_sum hs
      on d.station_id = hs.station_id
     and d.obs_date   = hs.obs_date
),

-- hourly rows that had no match in daily
right_part as (
    select
      hs.station_id,
      hs.obs_date,
      hs.hs_tavg_f       as tavg_f,
      hs.hs_tmin_f       as tmin_f,
      hs.hs_tmax_f       as tmax_f,
      hs.avg_dwpt_f,
      hs.min_dwpt_f,
      hs.max_dwpt_f,
      hs.avg_rhum_pct,
      hs.min_rhum_pct,
      hs.max_rhum_pct,
      hs.total_precip_mm as precipitation_mm,
      hs.total_snow_mm   as snow_mm,
      hs.avg_wind_dir_degrees as wind_dir_degrees,
      hs.avg_wind_speed_km_h  as wind_speed_km_h,
      hs.max_wind_peak_gust_km_h as wind_peak_gust_km_h,
      hs.avg_air_pressure_hpa as air_pressure_hpa,
      hs.total_sunshine_minutes as sunshine_minutes
    from hourly_sum hs
    left join daily d
      on d.station_id = hs.station_id
     and d.obs_date   = hs.obs_date
    where d.station_id is null
),

combined as (
    select * from left_part
    union all
    select * from right_part
),

final as (
    select
      {{ dbt_utils.generate_surrogate_key(['station_id','obs_date']) }} as station_date_id,
      station_id,
      obs_date,
      tavg_f,
      tmin_f,
      tmax_f,
      avg_dwpt_f,
      min_dwpt_f,
      max_dwpt_f,
      avg_rhum_pct,
      min_rhum_pct,
      max_rhum_pct,
      precipitation_mm,
      snow_mm,
      wind_dir_degrees,
      wind_speed_km_h,
      wind_peak_gust_km_h,
      air_pressure_hpa,
      sunshine_minutes
    from combined
)

select * from final
