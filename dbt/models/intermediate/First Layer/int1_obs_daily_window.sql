{{ config(
    materialized = "view",
    unique_key   = "station_obs_window_id"
) }}

with base as (
    select *
    from {{ ref('stg_obs_daily') }}
),

date_expansion as (
    select distinct
      station_id,
      obs_date as anchor_date
    from base
),

date_windows as (
    select
      anchor.station_id,
      anchor.anchor_date,
      obs.obs_date,
      obs.tavg_f,
      obs.tmin_f,
      obs.tmax_f,
      obs.precipitation_mm,
      obs.snow_mm,
      obs.wind_dir_degrees,
      obs.wind_speed_km_h,
      obs.wind_peak_gust_km_h,
      obs.air_pressure_hpa,
      obs.sunshine_minutes
    from date_expansion anchor
    join base obs
      on obs.station_id = anchor.station_id
     and obs.obs_date between anchor.anchor_date - interval '2 day' and anchor.anchor_date + interval '2 day'
),

final as (
    select
      {{ dbt_utils.generate_surrogate_key(['station_id', 'anchor_date', 'obs_date']) }} as station_obs_window_id,
      *
    from date_windows
)

select * from final
