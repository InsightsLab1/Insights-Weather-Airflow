{{ config(
    materialized = "table"
) }}

with

  raw as (
      select *
      from {{ ref('int2_obs_daily_all') }}
  ),

  final as (
      select
          station_date_id,
          station_id,
          obs_date,
          tavg_f,
          tmin_f,
          tmax_f,
          avg_dwpt_f,
          avg_rhum_pct,
          min_rhum_pct,
          max_rhum_pct,
          precipitation_mm as precip_mm,
          snow_mm            as snow_mm,
          wind_dir_degrees   as wind_dir_deg,
          wind_speed_km_h    as wind_speed_kmh,
          wind_peak_gust_km_h as wind_gust_kmh,
          air_pressure_hpa   as pressure_hpa,
          sunshine_minutes   as sunshine_min
      from raw
  )

select
  f.station_date_id,
  f.station_id,
  f.obs_date,
  unpiv.metric_name,
  unpiv.metric_value
from final as f
-- unpivot metrics into tall format
cross join lateral (
  values
    ('tavg_f',        f.tavg_f),
    ('tmin_f',        f.tmin_f),
    ('tmax_f',        f.tmax_f),
    ('avg_dwpt_f',    f.avg_dwpt_f),
    ('avg_rhum_pct',  f.avg_rhum_pct),
    ('min_rhum_pct',  f.min_rhum_pct),
    ('max_rhum_pct',  f.max_rhum_pct),
    ('precip_mm',     f.precip_mm),
    ('snow_mm',       f.snow_mm),
    ('wind_dir_deg',  f.wind_dir_deg),
    ('wind_speed_kmh',f.wind_speed_kmh),
    ('wind_gust_kmh', f.wind_gust_kmh),
    ('pressure_hpa',  f.pressure_hpa),
    ('sunshine_min',  f.sunshine_min)
) as unpiv(metric_name, metric_value)