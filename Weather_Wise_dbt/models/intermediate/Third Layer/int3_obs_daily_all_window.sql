{{ config(
    materialized = "view",
    unique_key   = "station_obs_window_id"
) }}

-- 1) Read the combined daily+hourly summary
with base as (
  select *  
  from {{ ref('int2_obs_daily_all') }}
),

-- 2) Generate one anchor date per station per observation date
date_expansion as (
  select distinct
    station_id,
    obs_date as anchor_date
  from base
),

-- 3) For each station anchor date, pull plus or minus 2 days of all metrics and QA flags
date_windows as (
  select
    anchor.station_id,
    anchor.anchor_date,
    obs.obs_date,

-- metrics
    obs.tavg_f,
    obs.tmin_f,
    obs.tmax_f,
    obs.avg_dwpt_f,
    obs.avg_rhum_pct,
    obs.min_rhum_pct,
    obs.max_rhum_pct,
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
   and obs.obs_date between anchor.anchor_date - interval '2 day'
                        and anchor.anchor_date + interval '2 day'
),

-- 4) Assign a surrogate key per station-anchor-obs triple
final as (
  select
    {{ dbt_utils.generate_surrogate_key(['station_id','anchor_date','obs_date']) }}
      as station_obs_window_id,
    *
  from date_windows
)

select * from final
