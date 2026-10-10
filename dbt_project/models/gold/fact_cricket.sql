{{ config(materialized='table',schema='gold') }}

select
    delivery_id,
    delivery_timestamp,
    innings_no,
    over_no,
    ball_in_over,
    runs_off_bat,
    extra_runs,
    total_runs,
    is_wicket,
    ball_speed_kmph,
    non_striker_id,
    extra_type,
    shot_type,
    shot_region,
    match_id,
    dismissal_type,
    venue_id,
    batting_team_id,
    bowling_team_id,
    batsman_id,
    bowler_id,
    drs_review_id
from {{ ref('silver_cricket_data') }}
