{{ config(materialized='view') }}

with cal as (
    select
        listing_id,
        date,
        available,
        reservation_id,
        price,
        minimum_nights,
        maximum_nights
    from {{ ref('stg_calendar') }}
),
dim as (
  select * from {{ ref('dim_listings') }}
)

select
    c.date,
    c.listing_id,
    d.listing_name,
    d.neighborhood,
    d.property_type,
    d.room_type,
    d.accommodates,
    c.available,
    (c.reservation_id is not null) as is_reserved,
    c.price,
    case 
        when c.reservation_id is not null then c.price 
        else 0 
    end as revenue,
    d.reviews_count,
    d.avg_review_score,
    d.has_wifi,
    d.has_air_conditioning,
    d.has_kitchen
from cal c
left join dim d
  on c.listing_id = d.listing_id
