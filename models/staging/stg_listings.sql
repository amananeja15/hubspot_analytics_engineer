{{ config(materialized='view') }}

with raw as (
  select * from {{ source('hubspot_raw', 'LISTINGS_RAW') }}
)


select
    nullif(regexp_replace(id, '[^0-9]', ''), '')::integer as listing_id,
  	nullif(regexp_replace(host_id, '[^0-9]', ''), '')::integer as host_id,
  	trim(name) as listing_name,
  	trim(host_name) as host_name,
    host_since,
  	host_location,
    coalesce(neighborhood, '') as neighborhood,
    property_type,
  	room_type,
    nullif(regexp_replace(accommodates, '[^0-9]', ''), '')::integer as accommodates,
  	bathrooms_text as bathrooms_text,
  	nullif(regexp_replace(bedrooms, '[^0-9.]', ''), '')::numeric as bedrooms,
  	nullif(regexp_replace(beds, '[^0-9.]', ''), '')::numeric as beds,
  	amenities as amenities_raw,
    nullif(regexp_replace(price, '[^0-9.]', ''), '')::numeric as list_price,
    nullif(regexp_replace(number_of_reviews, '[^0-9]', ''), '')::integer as number_of_reviews,
  	try_to_date(first_review) as first_review,
  	try_to_date(last_review) as last_review,
    case
        when coalesce(trim(to_varchar(review_scores_rating)), '') in ('', '0', 'NULL') then null
        else review_scores_rating
    end as review_scores_rating
from raw
