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
        CAST(round(tavg_f) AS SMALLINT) AS tavg_f,
        CAST(round(tmin_f) AS SMALLINT) AS tmin_f,
        CAST(round(tmax_f) AS SMALLINT) AS tmax_f,
        CAST(round(avg_dwpt_f)    AS SMALLINT) AS avg_dwpt_f,
        CAST(round(min_dwpt_f)    AS SMALLINT) AS min_dwpt_f,
        CAST(round(max_dwpt_f)    AS SMALLINT) AS max_dwpt_f,
        CAST(round(avg_rhum_pct)  AS SMALLINT) AS avg_rhum_pct,
        CAST(round(min_rhum_pct)  AS SMALLINT) AS min_rhum_pct,
        CAST(round(max_rhum_pct)  AS SMALLINT) AS max_rhum_pct,
        CAST(round(precip_mm)     AS FLOAT) AS precip_mm,
        CAST(round(snow_mm)       AS FLOAT) AS snow_mm,
--      CAST(wind_dir_deg  AS FLOAT) AS wind_dir_deg,
        CAST(wind_speed_kmh       AS FLOAT) AS wind_speed_kmh,
--      CAST(wind_gust_kmh AS FLOAT) AS wind_gust_kmh,
        CAST(round(pressure_hpa)  AS USMALLINT) AS pressure_hpa,
        CAST(round(sunshine_min)  AS USMALLINT) AS sunshine_min

    from raw
)

select * from final
