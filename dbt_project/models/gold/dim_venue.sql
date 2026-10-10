{{ config(materialized='table',schema='gold') }}

select distinct
    venue_id,
    venue_name,
    venue_city,
    venue_country
from {{ ref('silver_cricket_data') }}
where venue_id is not null
