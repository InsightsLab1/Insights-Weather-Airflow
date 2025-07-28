{{ config(
    materialized = "view",
    unique_key   = "station_city_match_id"
) }}

-- 1) Pull in station metadata, renaming lat/long for clarity
with stations as (
  select
    station_id,
    station_name,
    country_code,
    region_code,
    wmo_id,
    icao_code,
    latitude    as station_latitude,
    longitude   as station_longitude,
    elevation,
    timezone,
    hourly_start_date,
    hourly_end_date,
    daily_start_date,
    daily_end_date,
    monthly_start_date,
    monthly_end_date,
    snapshot_time
  from {{ ref('stg_obs_stations') }}
),

-- 2) Pull in city coordinate and population info, renaming lat/long for clarity
cities as (
  select
    city_coord_pop_id,
    uace,
    city_pop_id,
    total_matched_chars,
    matched_token_count,
    total_matched_chars_no_city,
    matched_token_count_no_city,
    pop_city_name,
    population_2023,
    coord_city_name,
    area_land_sqm    as city_area_land_sqm,
    area_water_sqm   as city_area_water_sqm,
    latitude         as city_latitude,
    longitude        as city_longitude
  from {{ ref('int1_city_coord_and_pop') }}
),

-- 3) Compute pairwise station,city distances using the Haversine formula
--    and filter to only those within 100 miles
station_city_haversine as (
  select
    s.*,
    c.*,
    2 * 3959 *
      asin(
        sqrt(
          power(sin(radians(c.city_latitude - s.station_latitude) / 2), 2)
          + cos(radians(s.station_latitude))
            * cos(radians(c.city_latitude))
            * power(sin(radians(c.city_longitude - s.station_longitude) / 2), 2)
        )
      ) as distance_miles
  from stations s
  cross join cities c
  where
    2 * 3959 *
      asin(
        sqrt(
          power(sin(radians(c.city_latitude - s.station_latitude) / 2), 2)
          + cos(radians(s.station_latitude))
            * cos(radians(c.city_latitude))
            * power(sin(radians(c.city_longitude - s.station_longitude) / 2), 2)
        )
      ) <= 100
),

-- 4) Rank each match by proximity in both directions:
--    station_to_city_proximity_rank = closest city per station
--    city_to_station_proximity_rank = closest station per city
ranked as (
  select
    {{ dbt_utils.generate_surrogate_key(['station_id','city_coord_pop_id']) }} as station_city_match_id,

-- station columns
    station_id,
    station_name,
    country_code,
    region_code,
    wmo_id,
    icao_code,
    station_latitude,
    station_longitude,
    elevation,
    timezone,
    hourly_start_date,
    hourly_end_date,
    daily_start_date,
    daily_end_date,
    monthly_start_date,
    monthly_end_date,
    snapshot_time,

    -- city columns
    city_coord_pop_id,
    uace,
    city_pop_id,
    total_matched_chars,
    matched_token_count,
    total_matched_chars_no_city,
    matched_token_count_no_city,
    pop_city_name,
    population_2023,
    coord_city_name,
    city_area_land_sqm,
    city_area_water_sqm,
    city_latitude,
    city_longitude,

    distance_miles,

-- proximity ranks
    row_number() over (
      partition by station_id
      order by distance_miles asc
    ) as station_to_city_proximity_rank,

    row_number() over (
      partition by city_coord_pop_id
      order by distance_miles asc
    ) as city_to_station_proximity_rank

  from station_city_haversine
)

-- 5) Final selection: list all columns plus the two proximity ranks
select
  station_city_match_id,
  station_id,
  station_name,
  country_code,
  region_code,
  wmo_id,
  icao_code,
  station_latitude,
  station_longitude,
  elevation,
  timezone,
  hourly_start_date,
  hourly_end_date,
  daily_start_date,
  daily_end_date,
  monthly_start_date,
  monthly_end_date,
  snapshot_time,
  city_coord_pop_id,
  uace,
  city_pop_id,
  total_matched_chars,
  matched_token_count,
  total_matched_chars_no_city,
  matched_token_count_no_city,
  pop_city_name,
  population_2023,
  coord_city_name,
  city_area_land_sqm,
  city_area_water_sqm,
  city_latitude,
  city_longitude,
  distance_miles,
  station_to_city_proximity_rank,
  city_to_station_proximity_rank
from ranked
order by station_id, station_to_city_proximity_rank
