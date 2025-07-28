{{ config(
    materialized = "table",
    unique_key   = "station_city_match_id"
) }}

with

raw as (
    select * 
    from {{ ref('int2_station_city_match') }}
),

final as (
    select
        station_city_match_id,
        station_id,
        station_name,
        country_code,
        region_code,
        wmo_id            as wmo,
        icao_code         as icao,
        station_latitude,
        station_longitude,
        elevation,
        timezone,
        hourly_start_date  as hourly_start,
        hourly_end_date    as hourly_end,
        daily_start_date   as daily_start,
        daily_end_date     as daily_end,
        monthly_start_date as monthly_start,
        monthly_end_date   as monthly_end,
        snapshot_time      as station_snapshot_time,
        city_coord_pop_id  as city_dim_id,
        uace               as area_code,
        city_pop_id        as pop_city_id,
        total_matched_chars         as match_chars,
        matched_token_count         as match_tokens,
        total_matched_chars_no_city as match_chars_no_city,
        matched_token_count_no_city as match_tokens_no_city,
        pop_city_name      as population_city,
        population_2023    as population,
        coord_city_name    as coordinate_city,
        city_area_land_sqm as land_area_sqm,
        city_area_water_sqm as water_area_sqm,
        city_latitude,
        city_longitude,
        distance_miles,
        station_to_city_proximity_rank as station_proximity_rank,
        city_to_station_proximity_rank as city_proximity_rank
    from raw
)

select * from final
