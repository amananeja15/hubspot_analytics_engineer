{{ config(materialized='view') }}

with src as (
  select * from {{ ref('stg_reviews') }}
)
select
  listing_id,
  count(*) as reviews_count,
  avg(review_score) as avg_review_score,
  max(review_date) as last_review_date,
from src
group by 1

