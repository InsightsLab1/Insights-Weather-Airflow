{{ config(
    materialized = "view",
    unique_key   = "obs_daily_id"
) }}

with raw as (
    select
      cast(time as date)                        as obs_date,
      station_id,
      tavg                        as tavg_c,
      tmin                        as tmin_c,
      tmax                        as tmax_c,
      prcp                        as precipitation_mm,
      snow                        as snow_mm,
      wdir                        as wind_dir_degrees,
      wspd                        as wind_speed_km_h,
      wpgt                        as wind_peak_gust_km_h,
      pres                        as air_pressure_hpa,
      tsun                        as sunshine_minutes,
      -- calculated fields
      {{ c_to_f('tavg_c') }} as tavg_f,
      {{ c_to_f('tmin_c') }} as tmin_f,
      {{ c_to_f('tmax_c') }} as tmax_f,
      -- drop load_time, any unwanted columns
      -- station columns removed here
      {{ dbt_utils.generate_surrogate_key(['station_id', 'obs_date']) }} as obs_daily_id,
    from {{ source('landing','land_obs_daily') }}
),
final as (
    select
      *
    from raw
)
select * from final
