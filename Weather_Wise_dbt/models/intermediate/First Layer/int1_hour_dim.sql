{{ config(
    materialized = "table",
    unique_key   = "hour"
) }}

select
  rng.range          as hour,
  lpad(rng.range::text, 2, '0') || ':00' as hour_label
from range(0, 23, 1) as rng(range)
