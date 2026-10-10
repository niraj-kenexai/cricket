{{ config(materialized='table', schema='gold') }}

with batsmen_rates as (
    select batsman_id as player_id,
        batsman_icc_rating as bat_rating,
        delivery_timestamp
    from {{ ref('silver_cricket_data') }}
    where batsman_id is not null and batsman_icc_rating is not null

),

bowler_rates as (
    select
        bowler_id as player_id,
        bowler_icc_rating as bowl_rating,
        delivery_timestamp
    from {{ ref('silver_cricket_data') }}
    where bowler_id is not null and bowler_icc_rating is not null

),

player_snapshots as (
    select
        player_id,
        bat_rating,
        CAST(null as int) as bowl_rating,
        delivery_timestamp
    from batsmen_rates

    UNION ALL

    select
        player_id,
        CAST(null as int) as bat_rating,
        bowl_rating,
        delivery_timestamp
    from bowler_rates
),

delivery_ratings as (
    select
        player_id,
        MAX(bat_rating) as bat_rating,
        MAX(bowl_rating) as bowl_rating,
        delivery_timestamp
    from player_snapshots
    group by player_id,delivery_timestamp
),

rating_states as (
    select
        player_id,
        bat_rating,
        bowl_rating,
        MIN(delivery_timestamp) as effective_from,
        MAX(delivery_timestamp) as latest_delivery_ts
    from delivery_ratings
    group by player_id,bat_rating,bowl_rating
),

rating_windows as (
    select
        player_id,
        bat_rating,
        bowl_rating,
        effective_from,
        LEAD(effective_from) OVER (PARTITION BY player_id ORDER BY effective_from) as effective_to,
        case
            when LEAD(effective_from) over (PARTITION BY player_id ORDER BY effective_from
            ) is null then TRUE
            else FALSE
        end as active_rating
    from rating_states

)

select
    ROW_NUMBER() OVER (ORDER BY player_id, effective_from) as rating_id,
    player_id,
    bat_rating,
    bowl_rating,
    effective_from,
    effective_to,
    active_rating
from rating_windows
