/*
Question: Q2 – Neighborhood Pricing (Price Increase YoY)
Purpose: Calculate the average price increase for each neighborhood from July 12th 2021 to July 11th 2022. 
Method: Compute price difference per listing (end – start), then average those differences within each neighborhood.
Source: fct_calendar_daily
Notes: Only include listings that have prices on both comparison dates.
*/

with two_days as (
  select
    listing_id,
    neighborhood,
    date,
    price
  from HUBSPOT_DB.DBT_AANEJA.FCT_CALENDAR_DAILY
  where date in ('2021-07-12', '2022-07-11')
),

pivoted as (
  select
    listing_id,
    neighborhood,
    max(case when date = '2021-07-12' then price end) as price_start,
    max(case when date = '2022-07-11' then price end) as price_end
  from two_days
  group by 1, 2
),

listing_diffs as (
  select
    neighborhood,
    listing_id,
    (price_end - price_start) as price_increase
  from pivoted
  where price_start is not null
    and price_end   is not null
)

select
  neighborhood,
  round(avg(price_increase), 2) as avg_price_increase,
  count(*) as listings_used
from listing_diffs
group by neighborhood
order by avg_price_increase desc;

