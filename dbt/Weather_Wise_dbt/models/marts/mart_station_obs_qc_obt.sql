{{ config(
    materialized = "table",
    unique_key   = "station_id"
) }}

with

stations as (
  select
    station_id,
    station_name,
    country_code,
    region_code,
    wmo_id,
    icao_code,
    latitude           as station_latitude,
    longitude          as station_longitude,
    elevation,
    timezone,
    hourly_start_date,
    hourly_end_date,
    daily_start_date,
    daily_end_date,
    monthly_start_date,
    monthly_end_date,
    snapshot_time      as station_snapshot_time
  from {{ ref('stg_obs_stations') }}
),

qc as (
  select *
  from {{ ref('int3_station_obs_qc') }}
),

final as (
  select
    qc.station_id,
    s.station_name,
    s.country_code,
    s.region_code,
    s.wmo_id,
    s.icao_code,
    s.station_latitude,
    s.station_longitude,
    s.elevation,
    s.timezone,
    s.hourly_start_date,
    s.hourly_end_date,
    s.daily_start_date,
    s.daily_end_date,
    s.monthly_start_date,
    s.monthly_end_date,
    qc.first_obs_date,
    qc.last_obs_date,
    qc.total_days,
    qc.data_span_days,
    qc.percent_days_reported,
    qc.total_gap_days,
    qc.long_gap_count,
    qc.pct_gap_days,
    qc.days_both_tavg_f,
    qc.days_only_daily_tavg_f,
    qc.days_only_hourly_tavg_f,
    qc.pct_matched_tavg_f,
    qc.pct_matched_tmin_f,
    qc.pct_matched_tmax_f,
    qc.pct_matched_precipitation_mm,
    qc.x_cnt_tavg_f,
    qc.x_cnt_tmin_f,
    qc.x_cnt_tmax_f,
    qc.x_cnt_precipitation_mm,
    qc.avg_abs_diff_tavg_f,
    qc.max_abs_diff_tavg_f,
    qc.avg_abs_diff_tmin_f,
    qc.max_abs_diff_tmin_f,
    qc.avg_abs_diff_tmax_f,
    qc.max_abs_diff_tmax_f,
    qc.avg_diff_tavg_f,
    qc.avg_diff_tmin_f,
    qc.avg_diff_tmax_f,
    qc.count_zero_precip_days,
    qc.count_no_sunshine_days,
    qc.count_out_of_range_tavg_f,
    qc.count_out_of_range_tmin_f,
    qc.count_out_of_range_tmax_f
  from qc
  left join stations s
    on qc.station_id = s.station_id
)

select * from final
