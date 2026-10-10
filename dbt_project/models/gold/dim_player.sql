{{ config(materialized='table', schema='gold') }}

with batsmen as (
    select DISTINCT
        batsman_id AS player_id,
        batsman_name AS player_name,
        batsman_dob AS date_of_birth,
        batsman_nationality AS nationality,
        batting_hand,
        batsman_role AS player_role,
        batsman_email AS email,
        batsman_phone AS phone,
        batsman_icc_rating,
        contract_id,
        contract_grade,
        annual_retainer_usd,
        contract_status
    from {{ ref('silver_cricket_data') }}
    where batsman_id IS NOT NULL

),

bowlers as (
    select DISTINCT
        bowler_id AS player_id,
        bowler_name AS player_name,
        bowler_nationality AS nationality,
        bowling_style,
        bowler_icc_rating
    from {{ ref('silver_cricket_data') }}
    where bowler_id is not null

)

select
    coalesce(b.player_id, bw.player_id) AS player_id,
    coalesce(b.player_name, bw.player_name) AS player_name,
    b.date_of_birth,
    coalesce(b.nationality, bw.nationality) AS nationality,
    b.batting_hand,
    bw.bowling_style,
    b.player_role,
    b.email,
    b.phone,
    b.batsman_icc_rating,
    bw.bowler_icc_rating,
    b.contract_id,
    b.contract_grade,
    b.annual_retainer_usd,
    b.contract_status
from batsmen b
full outer join bowlers bw on b.player_id = bw.player_id
