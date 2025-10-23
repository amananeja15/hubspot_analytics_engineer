{{ config(materialized='view') }}


with raw as (
  select * from {{ source('hubspot_raw', 'GENERATED_REVIEWS_RAW') }}
),

norm as (
  select
    *,
    to_varchar(id)              as id_v,
    to_varchar(listing_id)      as listing_id_v,
    to_varchar(review_score)    as review_score_v,
    to_varchar(review_date)            as review_date_v,
  from raw
)

select
  nullif(regexp_replace(id_v, '[^0-9]', ''), '')::integer as review_id,
  nullif(regexp_replace(listing_id_v, '[^0-9]', ''), '')::integer as listing_id,
  try_to_date(review_date_v) as review_date,
  case
    when try_cast(review_score_v as numeric) is not null
      then try_cast(review_score_v as numeric)
    else null
  end as review_score,
from norm
where review_id is NOT NULL
and review_date is NOT NULL
