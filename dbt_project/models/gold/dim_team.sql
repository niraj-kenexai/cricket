{{ config(materialized='table',schema='gold') }}

select DISTINCT
    batting_team_id as team_id,
    batting_team_name as team_name
from {{ ref('silver_cricket_data') }}
where batting_team_id is not null

union

select DISTINCT
    bowling_team_id as team_id,
    bowling_team_name as team_name
from {{ ref('silver_cricket_data') }}
where bowling_team_id s not null
