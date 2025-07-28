{{ config(
    materialized = "table",
    unique_key   = "city_coord_pop_id"
) }}

with

raw as (
    select * 
    from {{ ref('int1_city_coord_and_pop') }}
),

final as (
    select
        city_coord_pop_id,
        uace                      as area_code,
        city_pop_id               as pop_city_id,
        pop_city_name             as population_city,
        population_2023           as population,
        coord_city_name           as coordinate_city,
        area_land_sqm             as land_area_sqm,
        area_water_sqm            as water_area_sqm,
        latitude,
        longitude,
        total_matched_chars       as match_chars,
        matched_token_count       as match_tokens,
        total_matched_chars_no_city as match_chars_no_city,
        matched_token_count_no_city as match_tokens_no_city
    from raw
)

select * from final
