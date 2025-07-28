{{ config(
    materialized = "view",
    unique_key   = "uace"
) }}

with raw as (
    select
      UACE                     as uace,
      GEOID                    as geoid,
      GEOID_2                  as geoid_2,
      NAME                     as city_name,
      NAME_2                   as city_name_alt,
      AREA_LAND                as area_land_sqm,
      AREA_WATER               as area_water_sqm,
      LATITUDE                 as latitude,
      LONGITUDE                as longitude,
      last_load_time
    from {{ source('landing', 'land_city_coord') }}
),
final as (
    select *
    from raw
)

select * from final

