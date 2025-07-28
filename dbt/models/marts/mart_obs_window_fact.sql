{{ config(
    materialized = "table",
    unique_key   = "station_anchor_obs_id"
) }}
--todo: add checkpoint in cofig
with

raw as (
    select *
    from {{ ref('int3_obs_daily_all_window') }}
),

final as (
    select
        station_obs_window_id as station_anchor_obs_id,
        {{ dbt_utils.generate_surrogate_key(['station_id','anchor_date','obs_date']) }}
          as station_anchor_id,
        station_id,
        anchor_date,
        obs_date,
        tavg_f,
        tmin_f,
        tmax_f,
        avg_dwpt_f,
        avg_rhum_pct,
        min_rhum_pct,
        max_rhum_pct,
        precipitation_mm as precip_mm,
        snow_mm          as snow_mm,
        wind_dir_degrees as wind_dir_deg,
        wind_speed_km_h  as wind_speed_kmh,
        wind_peak_gust_km_h as wind_gust_kmh,
        air_pressure_hpa as pressure_hpa,
        sunshine_minutes as sunshine_min

    from raw
)

select * from final
