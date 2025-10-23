{{ config(materialized='view') }}

with src as (
  select *
  from {{ ref('stg_amenities_changelog') }}
),

exploded as (
    select
        s.listing_id,
        s.change_at,
        s.change_date,
        f.value::string as amenity_raw
    from src s,
    lateral flatten(input => try_parse_json(s.amenities_raw)) f
),

clean as (
    select
        listing_id,
        change_at,
        change_date,
            lower(
                trim(
                    regexp_replace(
                        regexp_replace(amenity_raw, '\\s+', ' '), 
                            '[^[:alnum:] ]', ''
                    )
                )
        ) as amenity
    from exploded
)

select 
    *
from clean
where amenity is not null
    qualify row_number() over (
    partition by listing_id, change_at, amenity
    order by change_at
    ) = 1
