{{ config(
    materialized = "table"
) }}

with

raw as (
    select * 
    from {{ ref('mart_obs_fact') }}
),

final as (
    select
        station_id,
        obs_date,
        CAST(tavg_f        AS FLOAT) AS tavg_f,
        CAST(tmin_f        AS FLOAT) AS tmin_f,
        CAST(tmax_f        AS FLOAT) AS tmax_f,
        CAST(avg_dwpt_f    AS FLOAT) AS avg_dwpt_f,
        CAST(min_dwpt_f    AS FLOAT) AS min_dwpt_f,
        CAST(max_dwpt_f    AS FLOAT) AS max_dwpt_f,
        CAST(avg_rhum_pct  AS FLOAT) AS avg_rhum_pct,
        CAST(min_rhum_pct  AS FLOAT) AS min_rhum_pct,
        CAST(max_rhum_pct  AS FLOAT) AS max_rhum_pct,
        CAST(precip_mm     AS FLOAT) AS precip_mm,
        CAST(snow_mm          AS FLOAT) AS snow_mm,
        CAST(wind_dir_deg     AS FLOAT) AS wind_dir_deg,
        CAST(wind_speed_kmh   AS FLOAT) AS wind_speed_kmh,
        CAST(wind_gust_kmh    AS FLOAT) AS wind_gust_kmh,
        CAST(pressure_hpa     AS FLOAT) AS pressure_hpa,
        CAST(sunshine_min     AS FLOAT) AS sunshine_min

    from raw
)

select * from final
