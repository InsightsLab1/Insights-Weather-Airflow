{{ config(
    materialized = "view",
    unique_key   = "station_id"
) }}

with

daily_all as (
  select * from {{ ref('int2_obs_daily_all_qc') }}
),

hourly_sum as (
  select
    station_id,
    obs_date,
    tavg_f           as hs_tavg_f,
    tmin_f           as hs_tmin_f,
    tmax_f           as hs_tmax_f,
    total_precip_mm  as hs_precipitation_mm,
    total_snow_mm    as hs_snow_mm,
    avg_wind_dir_degrees    as hs_wind_dir_degrees,
    avg_wind_speed_km_h     as hs_wind_speed_km_h,
    max_wind_peak_gust_km_h as hs_wind_peak_gust_km_h,
    avg_air_pressure_hpa    as hs_air_pressure_hpa,
    total_sunshine_minutes  as hs_sunshine_minutes
  from {{ ref('int1_obs_hourly_day_sum') }}
),

base as (
  select
    coalesce(d.station_id, h.station_id) as station_id,
    coalesce(d.obs_date,  h.obs_date)    as obs_date,

    d.tavg_f,      h.hs_tavg_f,
    d.tmin_f,      h.hs_tmin_f,
    d.tmax_f,      h.hs_tmax_f,
    d.precipitation_mm, h.hs_precipitation_mm,
    d.snow_mm,     h.hs_snow_mm,
    d.wind_dir_degrees, h.hs_wind_dir_degrees,
    d.wind_speed_km_h,  h.hs_wind_speed_km_h,
    d.wind_peak_gust_km_h, h.hs_wind_peak_gust_km_h,
    d.air_pressure_hpa, h.hs_air_pressure_hpa,
    d.sunshine_minutes, h.hs_sunshine_minutes,

    d.tavg_f_source,
    d.tmin_f_source,
    d.tmax_f_source,
    d.precipitation_mm_source,
    d.snow_mm_source,
    d.wind_dir_degrees_source,
    d.wind_speed_km_h_source,
    d.wind_peak_gust_km_h_source,
    d.air_pressure_hpa_source,
    d.sunshine_minutes_source,

    case when d.tavg_f_diff_pct is not null and abs(d.tavg_f_diff_pct) >= 0.05 then 0 else 1 end as tavg_f_match,

    case when d.tmin_f_diff_pct is not null and abs(d.tmin_f_diff_pct) >= 0.05 then 0 else 1 end as tmin_f_match,
    case when d.tmax_f_diff_pct is not null and abs(d.tmax_f_diff_pct) >= 0.05 then 0 else 1 end as tmax_f_match,
    case when d.precipitation_mm_diff_pct is not null and abs(d.precipitation_mm_diff_pct) >= 0.05 then 0 else 1 end as precipitation_mm_match,
    case when d.snow_mm_diff_pct is not null and abs(d.snow_mm_diff_pct) >= 0.05 then 0 else 1 end as snow_mm_match,
    case when d.wind_dir_degrees_diff_pct is not null and abs(d.wind_dir_degrees_diff_pct) >= 0.05 then 0 else 1 end as wind_dir_degrees_match,
    case when d.wind_speed_km_h_diff_pct is not null and abs(d.wind_speed_km_h_diff_pct) >= 0.05 then 0 else 1 end as wind_speed_km_h_match,
    case when d.wind_peak_gust_km_h_diff_pct is not null and abs(d.wind_peak_gust_km_h_diff_pct) >= 0.05 then 0 else 1 end as wind_peak_gust_km_h_match,
    case when d.air_pressure_hpa_diff_pct is not null and abs(d.air_pressure_hpa_diff_pct) >= 0.05 then 0 else 1 end as air_pressure_hpa_match,
    case when d.sunshine_minutes_diff_pct is not null and abs(d.sunshine_minutes_diff_pct) >= 0.05 then 0 else 1 end as sunshine_minutes_match,

  from daily_all d
  full join hourly_sum h
    on d.station_id = h.station_id
   and d.obs_date   = h.obs_date
),

date_range as (
  select
    station_id,
    min(obs_date) as first_obs_date,
    max(obs_date) as last_obs_date
  from base
  group by station_id
),

coverage as (
  select
    dr.station_id,
    dr.first_obs_date,
    dr.last_obs_date,
    datediff('day', dr.first_obs_date, dr.last_obs_date) + 1 as data_span_days,
    count(distinct b.obs_date) as total_days,
    count(distinct b.obs_date)::double
      / nullif(datediff('day', dr.first_obs_date, dr.last_obs_date) + 1, 0)
      as percent_days_reported
  from date_range dr
  left join base b
    on b.station_id = dr.station_id
  group by dr.station_id, dr.first_obs_date, dr.last_obs_date
),

gaps as (
  -- generate every date for each station, then mark gaps
  select
    dr.station_id,
    gs.gap_date,
    case
      when b.obs_date is null or b.tavg_f is null then 1
      else 0
    end as is_gap
  from date_range dr
  cross join generate_series(
    dr.first_obs_date,
    dr.last_obs_date,
    interval '1 day'
  ) as gs(gap_date)
  left join base b
    on b.station_id = dr.station_id
   and b.obs_date   = gs.gap_date
),

gap_flagged as (
  select
    station_id,
    gap_date,
    is_gap,
    case
      when is_gap = 1
        and coalesce(
          lag(is_gap) over (partition by station_id order by gap_date),
          0
        ) = 0
      then 1
      else 0
    end as is_new_gap_group
  from gaps
),

gap_runs as (
  select
    station_id,
    gap_date,
    is_gap,
    sum(is_new_gap_group) over (partition by station_id order by gap_date) as gap_group
  from gap_flagged
),

gap_summary as (
  select
    gr.station_id,
    sum(gr.is_gap) as total_gap_days,
    sum(gr.is_gap)::double
      / nullif(c.data_span_days,0) as pct_gap_days,
    count(*) filter (
      where gr.is_gap=1
        and grp_len >= 30
    ) as long_gap_count
  from gap_runs gr
  join coverage c
    on c.station_id = gr.station_id
  join (
    select station_id, gap_group, count(*) as grp_len
    from gap_runs
    where is_gap=1
    group by station_id, gap_group
  ) as grp_len
    on grp_len.station_id = gr.station_id
   and grp_len.gap_group  = gr.gap_group
  group by gr.station_id, c.data_span_days
),

stations as (
  select
    station_id,
    station_name,
    country_code,
    region_code,
    wmo_id,
    icao_code,
    latitude,
    longitude,
    elevation,
    timezone
  from {{ ref('stg_obs_stations') }}
),

qc as (
  select
    c.station_id,
    s.station_name,
    s.country_code,
    s.region_code,
    s.wmo_id,
    s.icao_code,
    s.latitude,
    s.longitude,
    s.elevation,
    s.timezone,

    c.first_obs_date,
    c.last_obs_date,
    c.total_days,
    c.data_span_days,
    c.percent_days_reported,
    g.total_gap_days,
    g.long_gap_count,
    g.pct_gap_days,

    sum(case when b.tavg_f_source = 'both'    then 1 else 0 end) as days_both_tavg_f,
    sum(case when b.tavg_f_source = 'daily'   then 1 else 0 end) as days_only_daily_tavg_f,
    sum(case when b.tavg_f_source = 'hourly'  then 1 else 0 end) as days_only_hourly_tavg_f,

    sum(case when b.tavg_f_match then 1 else 0 end)::double
      / nullif(sum(case when b.tavg_f_source = 'both' then 1 else 0 end),0) as pct_matched_tavg_f,
    sum(case when b.tmin_f_match then 1 else 0 end)::double
      / nullif(sum(case when b.tmin_f_source = 'both' then 1 else 0 end),0) as pct_matched_tmin_f,
    sum(case when b.tmax_f_match then 1 else 0 end)::double
      / nullif(sum(case when b.tmax_f_source = 'both' then 1 else 0 end),0) as pct_matched_tmax_f,
    sum(case when b.precipitation_mm_match then 1 else 0 end)::double
      / nullif(sum(case when b.precipitation_mm_source = 'both' then 1 else 0 end),0) as pct_matched_precipitation_mm,

    sum(case when b.tavg_f_source = 'both'    and not b.tavg_f_match    then 1 else 0 end) as x_cnt_tavg_f,
    sum(case when b.tmin_f_source = 'both'    and not b.tmin_f_match    then 1 else 0 end) as x_cnt_tmin_f,
    sum(case when b.tmax_f_source = 'both'    and not b.tmax_f_match    then 1 else 0 end) as x_cnt_tmax_f,
    sum(case when b.precipitation_mm_source = 'both' and not b.precipitation_mm_match then 1 else 0 end) as x_cnt_precipitation_mm,

    avg(abs(b.tavg_f - b.hs_tavg_f)) as avg_abs_diff_tavg_f,
    max(abs(b.tavg_f - b.hs_tavg_f)) as max_abs_diff_tavg_f,
    avg(abs(b.tmin_f - b.hs_tmin_f)) as avg_abs_diff_tmin_f,
    max(abs(b.tmin_f - b.hs_tmin_f)) as max_abs_diff_tmin_f,
    avg(abs(b.tmax_f - b.hs_tmax_f)) as avg_abs_diff_tmax_f,
    max(abs(b.tmax_f - b.hs_tmax_f)) as max_abs_diff_tmax_f,

    avg(b.hs_tavg_f - b.tavg_f) as avg_diff_tavg_f,
    avg(b.hs_tmin_f - b.tmin_f) as avg_diff_tmin_f,
    avg(b.hs_tmax_f - b.tmax_f) as avg_diff_tmax_f,

    sum(case when coalesce(b.precipitation_mm, b.hs_precipitation_mm) = 0 then 1 else 0 end) as count_zero_precip_days,
    sum(case when coalesce(b.sunshine_minutes, b.hs_sunshine_minutes) = 0 then 1 else 0 end) as count_no_sunshine_days,

    sum(case when b.tavg_f            not between -90 and 140 then 1 else 0 end) as count_out_of_range_tavg_f,
    sum(case when b.tmin_f            not between -90 and 140 then 1 else 0 end) as count_out_of_range_tmin_f,
    sum(case when b.tmax_f            not between -90 and 140 then 1 else 0 end) as count_out_of_range_tmax_f

  from base b
  join coverage c on c.station_id = b.station_id
  join gap_summary g on g.station_id = b.station_id
  join {{ ref('stg_obs_stations') }} s on s.station_id = b.station_id

  group by
    c.station_id,
    s.station_name,
    s.country_code,
    s.region_code,
    s.wmo_id,
    s.icao_code,
    s.latitude,
    s.longitude,
    s.elevation,
    s.timezone,
    c.first_obs_date,
    c.last_obs_date,
    c.total_days,
    c.data_span_days,
    c.percent_days_reported,
    g.total_gap_days,
    g.long_gap_count,
    g.pct_gap_days
)

select * from qc
