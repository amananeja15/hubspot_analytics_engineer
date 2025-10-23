/*
Question: Q3B – Maximum Possible Stay for Listings with Both Lockbox & First Aid Kit
Purpose: Compute the longest possible stay (same logic as Q3A) but only for listings that have BOTH 'lockbox' and 'first aid kit' amenities.
Method: Filter listings to those with both amenities using the exploded amenities model, then run the same max-stay calculation.
Source: int_amenities_exploded + stg_calendar
Window: July 12th 2021 to July 11th 2022
Notes: Expected to show a significantly shorter maximum stay vs Q3A.
*/

with amenity_listings as (
  select listing_id
  from HUBSPOT_DB.DBT_AANEJA.INT_AMENITIES_EXPLODED
  where amenity in ('lockbox', 'first aid kit')
  group by listing_id
  having count(distinct amenity) = 2
),
cal as (
  select
    c.listing_id,
    c.date,
    c.available,
    c.minimum_nights,
    c.maximum_nights
  from HUBSPOT_DB.DBT_AANEJA.STG_CALENDAR c
  join amenity_listings a
    on a.listing_id = c.listing_id
  where c.date between '2021-07-12' and '2022-07-11'
    and c.available = true
),
runs as (
  select
    listing_id,
    date,
    minimum_nights,
    maximum_nights,
    dateadd('day', -row_number() over (partition by listing_id order by date), date) as grp_key
  from cal
),
islands as (
  select
    listing_id,
    min(date) as start_date,
    max(date) as end_date,
    count(*)  as run_length
  from runs
  group by listing_id, grp_key
),
start_rules as (
  select
    i.listing_id,
    i.start_date,
    i.end_date,
    i.run_length,
    c.minimum_nights as min_nights_start,
    c.maximum_nights as max_nights_start
  from islands i
  join cal c
    on c.listing_id = i.listing_id
   and c.date = i.start_date
),
capped as (
  select
    listing_id,
    start_date,
    end_date,
    run_length,
    coalesce(min_nights_start, 1) as min_nights_start,
    coalesce(least(run_length, coalesce(max_nights_start, run_length)), 0) as feasible_length
  from start_rules
),
valid as (
  select
    listing_id,
    start_date,
    end_date,
    run_length,
    min_nights_start,
    feasible_length,
    case when feasible_length >= min_nights_start then feasible_length else 0 end as allowed_length
  from capped
),
ranked as (
  select
    listing_id,
    allowed_length as max_possible_stay_days,
    start_date as best_start_date,
    dateadd('day', allowed_length - 1, start_date) as best_end_date,
    row_number() over (
      partition by listing_id
      order by max_possible_stay_days desc, start_date asc
    ) as rn
  from valid
)
select
  listing_id,
  max_possible_stay_days,
  best_start_date,
  best_end_date
from ranked
where rn = 1
order by max_possible_stay_days desc, listing_id;
