{{ config(materialized='view') }}

with base as (
  select * from {{ ref('stg_listings') }}
),
latest_amenity_ts as (
  select listing_id, max(change_at) as latest_change_at
  from {{ ref('int_amenities_exploded') }}
  group by 1
),
latest_amenities as (
  select e.listing_id, e.amenity
  from {{ ref('int_amenities_exploded') }} e
  join latest_amenity_ts t
    on e.listing_id = t.listing_id
   and e.change_at = t.latest_change_at
),
amenity_flags as (
  select
    listing_id,
    max(amenity = 'wifi')::boolean             as has_wifi,
    max(amenity = 'air conditioning')::boolean as has_air_conditioning,
    max(amenity = 'kitchen')::boolean          as has_kitchen,
    count(*)                                   as amenity_count_latest
  from latest_amenities
  group by 1
),
with_reviews as (
  select
    b.listing_id,
    b.listing_name,
    b.host_id,
    b.host_name,
    b.host_since,
    b.host_location,
    b.neighborhood,
    b.property_type,
    b.room_type,
    b.accommodates,
    b.bathrooms_text,
    b.bedrooms,
    b.beds,
    b.list_price,
    b.number_of_reviews,
    b.first_review,
    b.last_review,
    b.review_scores_rating,
    r.reviews_count,
    r.avg_review_score,
    r.last_review_date
  from base b
  left join {{ ref('int_reviews_agg') }} r
    on b.listing_id = r.listing_id
)
select
  w.*,
  coalesce(a.has_wifi,false)             as has_wifi,
  coalesce(a.has_air_conditioning,false) as has_air_conditioning,
  coalesce(a.has_kitchen,false)          as has_kitchen,
  coalesce(a.amenity_count_latest, 0)    as amenity_count_latest
from with_reviews w
left join amenity_flags a
  on w.listing_id = a.listing_id
