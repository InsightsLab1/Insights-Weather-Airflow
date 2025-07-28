{{ config(
    materialized = "table",
    unique_key   = "date_key"
) }}

with

raw as (
    select * from {{ ref('int1_date_dim') }}
),

final as (
    select
        date_key,
        year,
        month,
        day_of_month,
        week_of_month,
        day_of_year,
        week_of_year,
        weekday
    from raw
)

select * from final
