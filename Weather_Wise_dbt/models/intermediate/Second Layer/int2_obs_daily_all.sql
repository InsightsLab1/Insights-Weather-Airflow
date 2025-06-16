{{ config(
    materialized = "view",
    unique_key   = "station_date_id"
) }}

with

daily as (
    select
      station_id,
      obs_date,
      tavg_c,
      tmin_c,
      tmax_c,
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
      avg_temp_c,
      min_temp_c,
      max_temp_c,
      avg_dwpt_c,
      avg_dwpt_f,
      avg_rhum_pct,
      avg_air_pressure_hpa,
      avg_wind_dir_degrees,
      avg_wind_speed_km_h,
      max_wind_peak_gust_km_h,
      total_precip_mm,
      total_snow_mm,
      total_sunshine_minutes
    from {{ ref('int1_obs_hourly_day_sum') }}
),

combined as (
    select
      coalesce(d.station_id, hs.station_id) as station_id,
      coalesce(d.obs_date,   hs.obs_date)   as obs_date,

      -- merged metrics
      coalesce(d.tavg_c,      hs.avg_temp_c)        as avg_temp_c,
      coalesce(d.tmin_c,      hs.min_temp_c)        as min_temp_c,
      coalesce(d.tmax_c,      hs.max_temp_c)        as max_temp_c,
      hs.avg_dwpt_c                                    as avg_dwpt_c,
      hs.avg_dwpt_f                                    as avg_dwpt_f,
      hs.avg_rhum_pct                                  as avg_rhum_pct,
      coalesce(d.precipitation_mm, hs.total_precip_mm) as precipitation_mm,
      coalesce(d.snow_mm,           hs.total_snow_mm)   as snow_mm,
      coalesce(d.wind_dir_degrees,   hs.avg_wind_dir_degrees)  as wind_dir_degrees,
      coalesce(d.wind_speed_km_h,    hs.avg_wind_speed_km_h)   as wind_speed_km_h,
      coalesce(d.wind_peak_gust_km_h,hs.max_wind_peak_gust_km_h) as wind_peak_gust_km_h,
      coalesce(d.air_pressure_hpa,   hs.avg_air_pressure_hpa)   as air_pressure_hpa,
      coalesce(d.sunshine_minutes,   hs.total_sunshine_minutes)  as sunshine_minutes,

      -- source indicators
      case
        when d.tavg_c   is not null and hs.avg_temp_c          is not null then 'both'
        when d.tavg_c   is not null                              then 'daily'
        when hs.avg_temp_c        is not null                    then 'hourly'
      end                                                  as avg_temp_c_source,
      case
        when d.tmin_c   is not null and hs.min_temp_c          is not null then 'both'
        when d.tmin_c   is not null                              then 'daily'
        when hs.min_temp_c        is not null                    then 'hourly'
      end                                                  as min_temp_c_source,
      case
        when d.tmax_c   is not null and hs.max_temp_c          is not null then 'both'
        when d.tmax_c   is not null                              then 'daily'
        when hs.max_temp_c        is not null                    then 'hourly'
      end                                                  as max_temp_c_source,
      case
        when d.precipitation_mm is not null and hs.total_precip_mm is not null then 'both'
        when d.precipitation_mm is not null                                then 'daily'
        when hs.total_precip_mm        is not null                          then 'hourly'
      end                                                  as precipitation_mm_source,
      case
        when d.snow_mm is not null and hs.total_snow_mm        is not null then 'both'
        when d.snow_mm is not null                                        then 'daily'
        when hs.total_snow_mm        is not null                          then 'hourly'
      end                                                  as snow_mm_source,
      case
        when d.wind_dir_degrees is not null and hs.avg_wind_dir_degrees is not null then 'both'
        when d.wind_dir_degrees is not null                                    then 'daily'
        when hs.avg_wind_dir_degrees   is not null                             then 'hourly'
      end                                                  as wind_dir_degrees_source,
      case
        when d.wind_speed_km_h is not null and hs.avg_wind_speed_km_h   is not null then 'both'
        when d.wind_speed_km_h is not null                                     then 'daily'
        when hs.avg_wind_speed_km_h   is not null                             then 'hourly'
      end                                                  as wind_speed_km_h_source,
      case
        when d.wind_peak_gust_km_h is not null and hs.max_wind_peak_gust_km_h is not null then 'both'
        when d.wind_peak_gust_km_h is not null                                      then 'daily'
        when hs.max_wind_peak_gust_km_h    is not null                              then 'hourly'
      end                                                  as wind_peak_gust_km_h_source,
      case
        when d.air_pressure_hpa is not null and hs.avg_air_pressure_hpa is not null then 'both'
        when d.air_pressure_hpa is not null                                    then 'daily'
        when hs.avg_air_pressure_hpa    is not null                             then 'hourly'
      end                                                  as air_pressure_hpa_source,
      case
        when d.sunshine_minutes is not null and hs.total_sunshine_minutes is not null then 'both'
        when d.sunshine_minutes is not null                                      then 'daily'
        when hs.total_sunshine_minutes  is not null                              then 'hourly'
      end                                                  as sunshine_minutes_source,

      -- match indicators
      case
        when d.tavg_c   is not null and hs.avg_temp_c          is not null then d.tavg_c   = hs.avg_temp_c
      end                                                  as avg_temp_c_match,
      case
        when d.tmin_c   is not null and hs.min_temp_c          is not null then d.tmin_c   = hs.min_temp_c
      end                                                  as min_temp_c_match,
      case
        when d.tmax_c   is not null and hs.max_temp_c          is not null then d.tmax_c   = hs.max_temp_c
      end                                                  as max_temp_c_match,
      case
        when d.precipitation_mm is not null and hs.total_precip_mm is not null then d.precipitation_mm = hs.total_precip_mm
      end                                                  as precipitation_mm_match,
      case
        when d.snow_mm is not null and hs.total_snow_mm        is not null then d.snow_mm = hs.total_snow_mm
      end                                                  as snow_mm_match,
      case
        when d.wind_dir_degrees is not null and hs.avg_wind_dir_degrees is not null then d.wind_dir_degrees = hs.avg_wind_dir_degrees
      end                                                  as wind_dir_degrees_match,
      case
        when d.wind_speed_km_h is not null and hs.avg_wind_speed_km_h   is not null then d.wind_speed_km_h = hs.avg_wind_speed_km_h
      end                                                  as wind_speed_km_h_match,
      case
        when d.wind_peak_gust_km_h is not null and hs.max_wind_peak_gust_km_h is not null then d.wind_peak_gust_km_h = hs.max_wind_peak_gust_km_h
      end                                                  as wind_peak_gust_km_h_match,
      case
        when d.air_pressure_hpa is not null and hs.avg_air_pressure_hpa    is not null then d.air_pressure_hpa = hs.avg_air_pressure_hpa
      end                                                  as air_pressure_hpa_match,
      case
        when d.sunshine_minutes is not null and hs.total_sunshine_minutes  is not null then d.sunshine_minutes = hs.total_sunshine_minutes
      end                                                  as sunshine_minutes_match

    from daily d
    full join hourly_sum hs
      on d.station_id = hs.station_id
     and d.obs_date   = hs.obs_date
),

final as (
    select
      {{ dbt_utils.generate_surrogate_key(['station_id','obs_date']) }} as station_date_id,
      station_id,
      obs_date,

      -- metrics
      avg_temp_c,
      min_temp_c,
      max_temp_c,
      avg_dwpt_c,
      avg_dwpt_f,
      avg_rhum_pct,
      precipitation_mm,
      snow_mm,
      wind_dir_degrees,
      wind_speed_km_h,
      wind_peak_gust_km_h,
      air_pressure_hpa,
      sunshine_minutes,

      -- source flags
      avg_temp_c_source,
      min_temp_c_source,
      max_temp_c_source,
      precipitation_mm_source,
      snow_mm_source,
      wind_dir_degrees_source,
      wind_speed_km_h_source,
      wind_peak_gust_km_h_source,
      air_pressure_hpa_source,
      sunshine_minutes_source,

      -- match flags
      avg_temp_c_match,
      min_temp_c_match,
      max_temp_c_match,
      precipitation_mm_match,
      snow_mm_match,
      wind_dir_degrees_match,
      wind_speed_km_h_match,
      wind_peak_gust_km_h_match,
      air_pressure_hpa_match,
      sunshine_minutes_match

    from combined
)

select * from final
