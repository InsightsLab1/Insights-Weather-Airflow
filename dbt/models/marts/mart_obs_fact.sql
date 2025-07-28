{{ config(
    materialized = "table"
) }}
--todo: add checkpoint in cofig
with

raw as (
    select * 
    from {{ ref('int2_obs_daily_all') }}
),

final as (
    select
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
