{{ config(
    materialized = "view",
    unique_key   = "date_key"
) }}

with bounds as (
  select
    min(obs_date)::date                     as start_date,
    current_date() + interval '10 year'     as end_date
  from {{ ref('stg_obs_daily') }}
)

select
  cast(rng.range as date)                                as date_key,
  extract(year    from date_key)                         as year,
  extract(month   from date_key)                         as month,
  extract(day     from date_key)                         as day_of_month,
  floor((extract(day from date_key) - 1) / 7) + 1        as week_of_month,
  extract(doy     from date_key)                         as day_of_year,
  extract(week    from date_key)                         as week_of_year,
  extract(dow     from date_key)                         as weekday
from bounds
cross join range(bounds.start_date, bounds.end_date, interval '1 day') as rng(range)
