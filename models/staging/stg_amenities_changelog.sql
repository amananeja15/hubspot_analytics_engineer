{{ config(materialized='view') }}

with raw as (
  select * from {{ source('hubspot_raw', 'AMENITIES_CHANGELOG_RAW') }}
),
norm as (
  select
    to_varchar(listing_id)  as listing_id_v,
    to_varchar(change_at)   as change_at_v,
    to_varchar(amenities)   as amenities_v
  from raw
)
select
  nullif(regexp_replace(listing_id_v, '[^0-9]', ''), '')::integer as listing_id,
  try_to_timestamp(change_at_v)                                  as change_at,
  try_to_timestamp(change_at_v)::date                            as change_date,
  trim(amenities_v)                                              as amenities_raw
from norm
where listing_id is not null
and try_to_timestamp(change_at_v) is not null
