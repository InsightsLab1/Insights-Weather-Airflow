{{ config(
    materialized = "table",
    unique_key   = "hour"
) }}

with

raw as (
    select
      hour,
      hour_label
    from {{ ref('int1_hour_dim') }}
),

final as (
    select
      hour,
      hour_label
    from raw
)

select * from final
