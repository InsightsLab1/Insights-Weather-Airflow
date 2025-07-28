{{ config(
    materialized = "view",
    unique_key   = "city_pop_id"
) }}

with raw as (
    select
      row_number() over (order by Geographic_Area) as city_pop_id,
      Geographic_Area             as city_name,
      cast(
      regexp_replace(Population_In_2023, '[^0-9]+', '', 'g')
      as bigint
    )                                              as population_2023
    from {{ source('landing', 'land_city_pop') }}
),
final as (
    select *
    from raw
)

select * from final