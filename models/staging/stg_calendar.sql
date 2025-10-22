{{ config(materialized='view') }}

with raw as (
  select * from {{ source('hubspot_raw', 'CALENDAR_RAW') }}
),

norm as (
  select
    *,
    lower(trim(coalesce(TO_VARCHAR(AVAILABLE), ''))) as available_raw,
    TO_VARCHAR(LISTING_ID) as listing_id_v,
    TO_VARCHAR(PRICE) as price_v,
    TO_VARCHAR(MINIMUM_NIGHTS) as min_nights_v,
    TO_VARCHAR(MAXIMUM_NIGHTS) as max_nights_v,
    TO_VARCHAR(RESERVATION_ID) as reservation_id_v
  from raw
)

select
    nullif(regexp_replace(listing_id_v, '[^0-9]', ''), '')::integer as listing_id,
    try_to_date("DATE") as date,
    case
        when available_raw in ('t','true','yes','y','1') then true
        when available_raw in ('f','false','no','n','0') then false
        when available_raw = '' then null
        else null
    end as available, 
    nullif(regexp_replace(reservation_id_v, '[^0-9]', ''), '')::integer as reservation_id,
    nullif(regexp_replace(price_v, '[$,]', ''), '')::numeric as price,
    nullif(regexp_replace(min_nights_v, '[^0-9]', ''), '')::integer as minimum_nights,
    nullif(regexp_replace(max_nights_v, '[^0-9]', ''), '')::integer as maximum_nights,
    price_v as price_raw
from norm

