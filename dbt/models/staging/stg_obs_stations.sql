{{ config(
    materialized = "view",
    unique_key   = "station_id"
) }}

with raw as (
    select
      id                         as station_id,
      name                       as station_name,
      country                    as country_code,
      region                     as region_code,
      wmo                        as wmo_id,
      icao                       as icao_code,
      latitude,
      longitude,
      elevation,
      timezone,
      cast(hourly_start as date)  as hourly_start_date,
      cast(hourly_end as date)    as hourly_end_date,
      cast(daily_start as date)   as daily_start_date,
      cast(daily_end as date)     as daily_end_date,
      cast(monthly_start as date) as monthly_start_date,
      cast(monthly_end as date)   as monthly_end_date,
      snapshot_time               as snapshot_time
    from {{ source('landing', 'land_obs_stations') }}
),

latest_snapshot as (
    select max(snapshot_time) as snapshot_time
    from raw
),

final as (
    select 
    r.*,
  CASE
    WHEN region_code = 'AL' THEN 'Alabama'
    WHEN region_code = 'AK' THEN 'Alaska'
    WHEN region_code = 'AZ' THEN 'Arizona'
    WHEN region_code = 'AR' THEN 'Arkansas'
    WHEN region_code = 'CA' THEN 'California'
    WHEN region_code = 'CO' THEN 'Colorado'
    WHEN region_code = 'CT' THEN 'Connecticut'
    WHEN region_code = 'DE' THEN 'Delaware'
    WHEN region_code = 'FL' THEN 'Florida'
    WHEN region_code = 'GA' THEN 'Georgia'
    WHEN region_code = 'HI' THEN 'Hawaii'
    WHEN region_code = 'ID' THEN 'Idaho'
    WHEN region_code = 'IL' THEN 'Illinois'
    WHEN region_code = 'IN' THEN 'Indiana'
    WHEN region_code = 'IA' THEN 'Iowa'
    WHEN region_code = 'KS' THEN 'Kansas'
    WHEN region_code = 'KY' THEN 'Kentucky'
    WHEN region_code = 'LA' THEN 'Louisiana'
    WHEN region_code = 'ME' THEN 'Maine'
    WHEN region_code = 'MD' THEN 'Maryland'
    WHEN region_code = 'MA' THEN 'Massachusetts'
    WHEN region_code = 'MI' THEN 'Michigan'
    WHEN region_code = 'MN' THEN 'Minnesota'
    WHEN region_code = 'MS' THEN 'Mississippi'
    WHEN region_code = 'MO' THEN 'Missouri'
    WHEN region_code = 'MT' THEN 'Montana'
    WHEN region_code = 'NE' THEN 'Nebraska'
    WHEN region_code = 'NV' THEN 'Nevada'
    WHEN region_code = 'NH' THEN 'New Hampshire'
    WHEN region_code = 'NJ' THEN 'New Jersey'
    WHEN region_code = 'NM' THEN 'New Mexico'
    WHEN region_code = 'NY' THEN 'New York'
    WHEN region_code = 'NC' THEN 'North Carolina'
    WHEN region_code = 'ND' THEN 'North Dakota'
    WHEN region_code = 'OH' THEN 'Ohio'
    WHEN region_code = 'OK' THEN 'Oklahoma'
    WHEN region_code = 'OR' THEN 'Oregon'
    WHEN region_code = 'PA' THEN 'Pennsylvania'
    WHEN region_code = 'RI' THEN 'Rhode Island'
    WHEN region_code = 'SC' THEN 'South Carolina'
    WHEN region_code = 'SD' THEN 'South Dakota'
    WHEN region_code = 'TN' THEN 'Tennessee'
    WHEN region_code = 'TX' THEN 'Texas'
    WHEN region_code = 'UT' THEN 'Utah'
    WHEN region_code = 'VT' THEN 'Vermont'
    WHEN region_code = 'VA' THEN 'Virginia'
    WHEN region_code = 'WA' THEN 'Washington'
    WHEN region_code = 'WV' THEN 'West Virginia'
    WHEN region_code = 'WI' THEN 'Wisconsin'
    WHEN region_code = 'WY' THEN 'Wyoming'
    WHEN region_code = 'DC' THEN 'District of Columbia'
    WHEN region_code = 'AS' THEN 'American Samoa'
    WHEN region_code = 'GU' THEN 'Guam'
    WHEN region_code = 'MP' THEN 'Northern Mariana Islands'
    WHEN region_code = 'PR' THEN 'Puerto Rico'
    WHEN region_code = 'VI' THEN 'U.S. Virgin Islands'
    ELSE NULL
  END AS region_name
    from raw r
    where r.snapshot_time = (select snapshot_time from latest_snapshot)
)

select * from final
