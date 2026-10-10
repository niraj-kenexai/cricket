{{ config(materialized='table',schema='gold') }}

select DISTINCT
    match_id,
    match_date,
    match_format,
    tournament_name,
    season,
    match_stage,
    toss_decision,
    venue_id
from {{ ref('silver_cricket_data') }}
where match_id is not null
