{{ config(
    materialized = "view",
    unique_key   = "obs_daily_id"
) }}

with raw as (
    select
      time                        as obs_time,
      station_id,
      tavg                        as temp_c,
      tmin                        as temp_min_c,
      tmax                        as temp_max_c,
      prcp,
      snow,
      wdir,
      wspd,
      wpgt,
      pres,
      tsun,
      -- calculated fields
      (tavg * 9.0/5.0 + 32.0)     as temp_f,
      (tmin * 9.0/5.0 + 32.0)     as temp_min_f,
      (tmax * 9.0/5.0 + 32.0)     as temp_max_f,
      -- drop load_time, any unwanted columns
      -- station columns removed here
      concat(station_id, '_', to_char(time, 'YYYYMMDDHH24')) as obs_daily_id
    from {{ source('landing', 'land_obs_daily') }}
),

-- ensure you only keep the latest snapshot per record, etc.
final as (
    select
      *
    from raw
    where obs_time is not null
)

select * from final;
