/*
Question: Q1 – Revenue split by AC vs Non-AC (by month)
Purpose: Show total revenue and % share per month, segmented by whether listings have air conditioning.
Source: HUBSPOT_DB.DBT_AANEJA.FCT_CALENDAR_DAILY (fact table)
Notes: Revenue is price only when reserved; uses latest-snapshot amenity flag from dim_listings.
*/

with monthly as (
    select
        date_trunc('month', date) as month,
        has_air_conditioning,
        sum(revenue) as revenue_total
    from HUBSPOT_DB.DBT_AANEJA.FCT_CALENDAR_DAILY
    group by 1, 2
),
with_pct as (
    select
        month,
        has_air_conditioning,
        revenue_total,
        sum(revenue_total) over (partition by month) as month_revenue_total
    from monthly
)
select
    month,
    case 
        when has_air_conditioning then 'with_ac' 
        else 'without_ac' 
    end as ac_segment,
    revenue_total,
    round(100 * revenue_total / nullif(month_revenue_total, 0), 1) as pct_revenue
from with_pct
order by month, ac_segment desc;
