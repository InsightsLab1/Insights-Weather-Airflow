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

left_part as (
    select
      coalesce(d.station_id, hs.station_id) as station_id,
      coalesce(d.obs_date,   hs.obs_date)   as obs_date,

      coalesce(d.tavg_f, hs.hs_tavg_f)   as tavg_f,
      coalesce(d.tmin_f, hs.hs_tmin_f)   as tmin_f,
      coalesce(d.tmax_f, hs.hs_tmax_f)   as tmax_f,
      hs.avg_dwpt_f,
      hs.min_dwpt_f,
      hs.max_dwpt_f,
      hs.avg_rhum_pct,
      hs.min_rhum_pct,
      hs.max_rhum_pct,
      hs.avg_air_pressure_hpa,
      hs.avg_wind_dir_degrees,
      hs.avg_wind_speed_km_h,
      hs.max_wind_peak_gust_km_h,
      coalesce(d.precipitation_mm, hs.total_precip_mm) as precipitation_mm,
      coalesce(d.snow_mm, hs.total_snow_mm) as snow_mm,
      coalesce(d.wind_dir_degrees, hs.avg_wind_dir_degrees) as wind_dir_degrees,
      coalesce(d.wind_speed_km_h, hs.avg_wind_speed_km_h) as wind_speed_km_h,
      coalesce(d.wind_peak_gust_km_h, hs.max_wind_peak_gust_km_h) as wind_peak_gust_km_h,
      coalesce(d.air_pressure_hpa, hs.avg_air_pressure_hpa) as air_pressure_hpa,
      coalesce(d.sunshine_minutes, hs.total_sunshine_minutes) as sunshine_minutes,

      -- Source flags
      case when d.tavg_f is not null and hs.hs_tavg_f is not null then 'both'
           when d.tavg_f is not null then 'daily'
           when hs.hs_tavg_f is not null then 'hourly' end as tavg_f_source,

      case when d.tmin_f is not null and hs.hs_tmin_f is not null then 'both'
           when d.tmin_f is not null then 'daily'
           when hs.hs_tmin_f is not null then 'hourly' end as tmin_f_source,

      case when d.tmax_f is not null and hs.hs_tmax_f is not null then 'both'
           when d.tmax_f is not null then 'daily'
           when hs.hs_tmax_f is not null then 'hourly' end as tmax_f_source,

      case when d.precipitation_mm is not null and hs.total_precip_mm is not null then 'both'
           when d.precipitation_mm is not null then 'daily'
           when hs.total_precip_mm is not null then 'hourly' end as precipitation_mm_source,

      case when d.snow_mm is not null and hs.total_snow_mm is not null then 'both'
           when d.snow_mm is not null then 'daily'
           when hs.total_snow_mm is not null then 'hourly' end as snow_mm_source,

      case when d.wind_dir_degrees is not null and hs.avg_wind_dir_degrees is not null then 'both'
           when d.wind_dir_degrees is not null then 'daily'
           when hs.avg_wind_dir_degrees is not null then 'hourly' end as wind_dir_degrees_source,

      case when d.wind_speed_km_h is not null and hs.avg_wind_speed_km_h is not null then 'both'
           when d.wind_speed_km_h is not null then 'daily'
           when hs.avg_wind_speed_km_h is not null then 'hourly' end as wind_speed_km_h_source,

      case when d.wind_peak_gust_km_h is not null and hs.max_wind_peak_gust_km_h is not null then 'both'
           when d.wind_peak_gust_km_h is not null then 'daily'
           when hs.max_wind_peak_gust_km_h is not null then 'hourly' end as wind_peak_gust_km_h_source,

      case when d.air_pressure_hpa is not null and hs.avg_air_pressure_hpa is not null then 'both'
           when d.air_pressure_hpa is not null then 'daily'
           when hs.avg_air_pressure_hpa is not null then 'hourly' end as air_pressure_hpa_source,

      case when d.sunshine_minutes is not null and hs.total_sunshine_minutes is not null then 'both'
           when d.sunshine_minutes is not null then 'daily'
           when hs.total_sunshine_minutes is not null then 'hourly' end as sunshine_minutes_source,

      -- Percent diffs with divide-by-zero protection
      case when d.tavg_f is not null and hs.hs_tavg_f is not null
           then (hs.hs_tavg_f - d.tavg_f) / d.tavg_f  end as tavg_f_diff_pct,

      case when d.tmin_f is not null and hs.hs_tmin_f is not null 
           then (hs.hs_tmin_f - d.tmin_f) / d.tmin_f end as tmin_f_diff_pct,

      case when d.tmax_f is not null and hs.hs_tmax_f is not null
           then (hs.hs_tmax_f - d.tmax_f) / d.tmax_f  end as tmax_f_diff_pct,

      case when d.precipitation_mm is not null and hs.total_precip_mm is not null 
           then (hs.total_precip_mm - d.precipitation_mm) / d.precipitation_mm end as precipitation_mm_diff_pct,

      case when d.snow_mm is not null and hs.total_snow_mm is not null
           then (hs.total_snow_mm - d.snow_mm) / d.snow_mm  end as snow_mm_diff_pct,

      case when d.wind_dir_degrees is not null and hs.avg_wind_dir_degrees is not null
           then (hs.avg_wind_dir_degrees - d.wind_dir_degrees) / d.wind_dir_degrees  end as wind_dir_degrees_diff_pct,

      case when d.wind_speed_km_h is not null and hs.avg_wind_speed_km_h is not null
           then (hs.avg_wind_speed_km_h - d.wind_speed_km_h) / d.wind_speed_km_h end as wind_speed_km_h_diff_pct,

      case when d.wind_peak_gust_km_h is not null and hs.max_wind_peak_gust_km_h is not null
           then (hs.max_wind_peak_gust_km_h - d.wind_peak_gust_km_h) / d.wind_peak_gust_km_h end as wind_peak_gust_km_h_diff_pct,

      case when d.air_pressure_hpa is not null and hs.avg_air_pressure_hpa is not null
           then (hs.avg_air_pressure_hpa - d.air_pressure_hpa) / d.air_pressure_hpa end as air_pressure_hpa_diff_pct,

      case when d.sunshine_minutes is not null and hs.total_sunshine_minutes is not null 
           then (hs.total_sunshine_minutes - d.sunshine_minutes) / d.sunshine_minutes end as sunshine_minutes_diff_pct

    from daily d
    left join hourly_sum hs
      on d.station_id = hs.station_id
     and d.obs_date = hs.obs_date
),

right_part as (
    select
      hs.station_id,
      hs.obs_date,
      hs.hs_tavg_f as tavg_f,
      hs.hs_tmin_f as tmin_f,
      hs.hs_tmax_f as tmax_f,
      hs.avg_dwpt_f,
      hs.min_dwpt_f,
      hs.max_dwpt_f,
      hs.avg_rhum_pct,
      hs.min_rhum_pct,
      hs.max_rhum_pct,
      hs.avg_air_pressure_hpa,
      hs.avg_wind_dir_degrees,
      hs.avg_wind_speed_km_h,
      hs.max_wind_peak_gust_km_h,
      hs.total_precip_mm as precipitation_mm,
      hs.total_snow_mm as snow_mm,
      hs.avg_wind_dir_degrees as wind_dir_degrees,
      hs.avg_wind_speed_km_h as wind_speed_km_h,
      hs.max_wind_peak_gust_km_h as wind_peak_gust_km_h,
      hs.avg_air_pressure_hpa as air_pressure_hpa,
      hs.total_sunshine_minutes as sunshine_minutes,

      -- default: only from hourly
      'hourly' as tavg_f_source,
      'hourly' as tmin_f_source,
      'hourly' as tmax_f_source,
      'hourly' as precipitation_mm_source,
      'hourly' as snow_mm_source,
      'hourly' as wind_dir_degrees_source,
      'hourly' as wind_speed_km_h_source,
      'hourly' as wind_peak_gust_km_h_source,
      'hourly' as air_pressure_hpa_source,
      'hourly' as sunshine_minutes_source,

      null as tavg_f_diff_pct,
      null as tmin_f_diff_pct,
      null as tmax_f_diff_pct,
      null as precipitation_mm_diff_pct,
      null as snow_mm_diff_pct,
      null as wind_dir_degrees_diff_pct,
      null as wind_speed_km_h_diff_pct,
      null as wind_peak_gust_km_h_diff_pct,
      null as air_pressure_hpa_diff_pct,
      null as sunshine_minutes_diff_pct

    from hourly_sum hs
    left join daily d
      on d.station_id = hs.station_id
     and d.obs_date = hs.obs_date
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
      *
    from combined
)

select * from final
