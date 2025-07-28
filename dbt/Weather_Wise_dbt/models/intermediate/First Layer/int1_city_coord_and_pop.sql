{{ config(
    materialized = "view",
    unique_key   = "city_coord_pop_id"
) }}

with

-- 0) USPS state name -> abbreviation mapping
state_map as (
  select *
  from (values
    ('Alabama','AL'),('Alaska','AK'),('Arizona','AZ'),('Arkansas','AR'),
    ('California','CA'),('Colorado','CO'),('Connecticut','CT'),('Delaware','DE'),
    ('Florida','FL'),('Georgia','GA'),('Hawaii','HI'),('Idaho','ID'),
    ('Illinois','IL'),('Indiana','IN'),('Iowa','IA'),('Kansas','KS'),
    ('Kentucky','KY'),('Louisiana','LA'),('Maine','ME'),('Maryland','MD'),
    ('Massachusetts','MA'),('Michigan','MI'),('Minnesota','MN'),('Mississippi','MS'),
    ('Missouri','MO'),('Montana','MT'),('Nebraska','NE'),('Nevada','NV'),
    ('New Hampshire','NH'),('New Jersey','NJ'),('New Mexico','NM'),('New York','NY'),
    ('North Carolina','NC'),('North Dakota','ND'),('Ohio','OH'),('Oklahoma','OK'),
    ('Oregon','OR'),('Pennsylvania','PA'),('Rhode Island','RI'),('South Carolina','SC'),
    ('South Dakota','SD'),('Tennessee','TN'),('Texas','TX'),('Utah','UT'),
    ('Vermont','VT'),('Virginia','VA'),('Washington','WA'),('West Virginia','WV'),
    ('Wisconsin','WI'),('Wyoming','WY')
  ) as t(state_name, state_abbr)
),

-- 1) Parse coordinate names and extract up to four state abbreviations
coord_fmt as (
  select
    uace,
    city_name                      as coord_city_name,
    area_land_sqm,
    area_water_sqm,
    latitude,
    longitude,
    trim(split_part(split_part(city_name, ',', 2), '--', 1)) as state_1,
    trim(split_part(split_part(city_name, ',', 2), '--', 2)) as state_2,
    trim(split_part(split_part(city_name, ',', 2), '--', 3)) as state_3,
    trim(split_part(split_part(city_name, ',', 2), '--', 4)) as state_4
  from {{ ref('stg_city_coord') }}
),

-- 2) Parse population names, extract full state name, look up its abbreviation
pop_fmt as (
  select
    city_pop_id,
    city_name                      as pop_city_name,
    population_2023,
    trim(split_part(city_name, ',', -1))        as pop_state_full,
    sm.state_abbr                              as pop_state_abbr
  from {{ ref('stg_city_pop') }} pf
  left join state_map sm
    on lower(trim(split_part(pf.city_name, ',', -1))) = lower(sm.state_name)
),

-- 3) Tokenize coordinate city names
coord_tokens as (
  select
    cf.uace,
    cf.state_1, cf.state_2, cf.state_3, cf.state_4,
    cf.coord_city_name,
    cf.area_land_sqm, cf.area_water_sqm, cf.latitude, cf.longitude,
    token
  from coord_fmt cf
  cross join unnest(
    regexp_split_to_array(lower(coord_city_name), '[^a-z0-9]+')
  ) as ct(token)
),

-- 4) Tokenize population city names
pop_tokens as (
  select
    pf.city_pop_id,
    pf.pop_state_abbr,
    pf.pop_city_name,
    pf.population_2023,
    token
  from pop_fmt pf
  cross join unnest(
    regexp_split_to_array(lower(pop_city_name), '[^a-z0-9]+')
  ) as pt(token)
),

-- 5) Match tokens only when state abbreviations align
matched_tokens as (
  select
    ct.uace,
    pt.city_pop_id,
    ct.token,
    length(ct.token) as token_length
  from coord_tokens ct
  join pop_tokens pt using(token)
  where ct.token <> ''
    and pt.pop_state_abbr in (ct.state_1, ct.state_2, ct.state_3, ct.state_4)
),

-- 6) Compute full and no-"city" scores
score_summary as (
  select
    uace,
    city_pop_id,
    sum(token_length) as total_matched_chars,
    count(*)          as matched_token_count
  from matched_tokens
  group by uace, city_pop_id
),
score_summary_no_city as (
  select
    uace,
    city_pop_id,
    sum(token_length) as total_matched_chars_no_city,
    count(*)          as matched_token_count_no_city
  from matched_tokens
  where lower(token) <> 'city'
  group by uace, city_pop_id
),

-- 7) Choose the best match per coordinate
ranked_matches as (
  select
    s.uace,
    s.city_pop_id,
    s.total_matched_chars,
    s.matched_token_count,
    row_number() over (
      partition by s.uace
      order by s.total_matched_chars desc, s.matched_token_count desc
    ) as rn_coord
  from score_summary s
),
best_matches as (
  select
    uace,
    city_pop_id,
    total_matched_chars,
    matched_token_count
  from ranked_matches
  where rn_coord = 1
),

-- 8) Rejoin to get full records plus no-city scores
city_coord_and_pop as (
  select
    bm.uace,
    bm.city_pop_id,
    bm.total_matched_chars,
    bm.matched_token_count,
    coalesce(nc.total_matched_chars_no_city, 0)   as total_matched_chars_no_city,
    coalesce(nc.matched_token_count_no_city, 0)   as matched_token_count_no_city,
    pf.pop_city_name,
    pf.population_2023,
    cf.coord_city_name,
    cf.area_land_sqm,
    cf.area_water_sqm,
    cf.latitude,
    cf.longitude
  from best_matches bm
  left join score_summary_no_city nc
    on nc.uace        = bm.uace
   and nc.city_pop_id = bm.city_pop_id
  inner join pop_fmt pf
    on bm.city_pop_id = pf.city_pop_id
  inner join coord_fmt cf
    on bm.uace        = cf.uace
),

-- 9) Final surrogate key and output
final as (
  select
    {{ dbt_utils.generate_surrogate_key(['uace','city_pop_id']) }} as city_coord_pop_id,
    *
  from city_coord_and_pop
)

select * from final
