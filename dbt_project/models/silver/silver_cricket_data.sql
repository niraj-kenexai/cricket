{{ config(materialized='view', schema='silver') }}

WITH raw_source AS (

    SELECT
        delivery_id,
        delivery_timestamp,
        innings_no,
        over_no,
        ball_in_over,
        runs_off_bat,
        extra_type,
        extra_runs,
        total_runs,
        is_wicket,
        dismissal_type,
        ball_speed_kmph,
        non_striker_id,
        shot_type,
        shot_region,
        match_id,
        match_date,
        match_format,
        tournament_name,
        season,
        match_stage,
        toss_decision,
        venue_id,
        venue_name,
        venue_city,
        venue_country,
        batting_team_id,
        batting_team_name,
        bowling_team_id,
        bowling_team_name,
        batsman_id,
        batsman_name,
        batsman_dob,
        batsman_nationality,
        batting_hand,
        batsman_role,
        batsman_email,
        batsman_phone,
        batsman_icc_rating,
        bowler_id,
        bowler_name,
        bowler_nationality,
        bowling_style,
        bowler_icc_rating,
        contract_id,
        contract_grade,
        annual_retainer_usd,
        contract_status,
        drs_review_id,
        drs_reviewed_by,
        drs_original_decision,
        drs_outcome,
        sourcefile,
        load_timestamp
    FROM {{ source('bronze', 'RAW_DATA') }}
    WHERE delivery_id IS NOT NULL 
      AND trim(delivery_id) <> ''

),

clean AS (

    SELECT
        SHA2(trim(delivery_id), 256) AS delivery_key,
        try_cast(trim(delivery_id) AS VARCHAR(50)) AS delivery_id,

        coalesce(
            try_to_timestamp(trim(delivery_timestamp), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp(trim(delivery_timestamp), 'MM/DD/YYYY HH24:MI:SS'),
            try_to_timestamp(trim(delivery_timestamp), 'DD/MM/YYYY HH24:MI:SS'),
            try_to_timestamp(trim(delivery_timestamp))
        ) AS delivery_timestamp,

        try_cast(trim(innings_no) AS INT) AS innings_no,
        try_cast(trim(over_no) AS INT) AS over_no,
        try_cast(trim(ball_in_over) AS INT) AS ball_in_over,

        try_cast(trim(runs_off_bat) AS INT) AS runs_off_bat,
        try_cast(nullif(trim(extra_type), '') AS VARCHAR(50)) AS extra_type,
        try_cast(trim(extra_runs) AS INT) AS extra_runs,
        try_cast(trim(total_runs) AS INT) AS total_runs,
        try_cast(trim(ball_speed_kmph) AS DECIMAL(6, 2)) AS ball_speed_kmph,

        try_cast(nullif(trim(shot_type), '') AS VARCHAR(50)) AS shot_type,
        try_cast(nullif(trim(shot_region), '') AS VARCHAR(50)) AS shot_region,

        try_cast(trim(is_wicket) AS BOOLEAN) AS is_wicket,
        try_cast(nullif(trim(dismissal_type), '') AS VARCHAR(50)) AS dismissal_type,
        try_cast(nullif(trim(non_striker_id), '') AS VARCHAR(50)) AS non_striker_id,

        try_cast(nullif(trim(match_id), '') AS VARCHAR(50)) AS match_id,
        coalesce(
            try_to_date(trim(match_date), 'YYYY-MM-DD'),
            try_to_date(trim(match_date), 'MM/DD/YYYY'),
            try_to_date(trim(match_date), 'DD/MM/YYYY'),
            try_to_date(trim(match_date))
        ) AS match_date,
        try_cast(nullif(trim(match_format), '') AS VARCHAR(50)) AS match_format,
        try_cast(nullif(trim(tournament_name), '') AS VARCHAR(100)) AS tournament_name,
        try_cast(nullif(trim(season), '') AS VARCHAR(50)) AS season,
        try_cast(nullif(trim(match_stage), '') AS VARCHAR(50)) AS match_stage,
        try_cast(nullif(trim(toss_decision), '') AS VARCHAR(50)) AS toss_decision,

        try_cast(nullif(trim(venue_id), '') AS VARCHAR(50)) AS venue_id,
        try_cast(nullif(trim(venue_name), '') AS VARCHAR(150)) AS venue_name,
        try_cast(nullif(trim(venue_city), '') AS VARCHAR(100)) AS venue_city,
        try_cast(nullif(trim(venue_country), '') AS VARCHAR(100)) AS venue_country,

        try_cast(nullif(trim(batting_team_id), '') AS VARCHAR(50)) AS batting_team_id,
        try_cast(nullif(trim(batting_team_name), '') AS VARCHAR(100)) AS batting_team_name,
        try_cast(nullif(trim(bowling_team_id), '') AS VARCHAR(50)) AS bowling_team_id,
        try_cast(nullif(trim(bowling_team_name), '') AS VARCHAR(100)) AS bowling_team_name,

        try_cast(nullif(trim(batsman_id), '') AS VARCHAR(50)) AS batsman_id,
        try_cast(nullif(trim(batsman_name), '') AS VARCHAR(100)) AS batsman_name,
        coalesce(
            try_to_date(trim(batsman_dob), 'YYYY-MM-DD'),
            try_to_date(trim(batsman_dob), 'MM/DD/YYYY'),
            try_to_date(trim(batsman_dob), 'DD/MM/YYYY'),
            try_to_date(trim(batsman_dob))
        ) AS batsman_dob,
        try_cast(nullif(trim(batsman_nationality), '') AS VARCHAR(50)) AS batsman_nationality,
        try_cast(nullif(trim(batting_hand), '') AS VARCHAR(50)) AS batting_hand,
        try_cast(nullif(trim(batsman_role), '') AS VARCHAR(50)) AS batsman_role,
        try_cast(LOWER(nullif(trim(batsman_email), '')) AS VARCHAR(150)) AS batsman_email,
        try_cast(nullif(trim(batsman_phone), '') AS VARCHAR(30)) AS batsman_phone,
        try_cast(nullif(trim(batsman_icc_rating), '') AS INT) AS batsman_icc_rating,
        
        try_cast(nullif(trim(bowler_id), '') AS VARCHAR(50)) AS bowler_id,
        try_cast(nullif(trim(bowler_name), '') AS VARCHAR(100)) AS bowler_name,
        try_cast(nullif(trim(bowler_nationality), '') AS VARCHAR(50)) AS bowler_nationality,
        try_cast(nullif(trim(bowling_style), '') AS VARCHAR(50)) AS bowling_style,
        try_cast(nullif(trim(bowler_icc_rating), '') AS INT) AS bowler_icc_rating,

        try_cast(nullif(trim(contract_id), '') AS VARCHAR(50)) AS contract_id,
        try_cast(nullif(trim(contract_grade), '') AS VARCHAR(50)) AS contract_grade,
        try_cast(nullif(trim(annual_retainer_usd), '') AS DECIMAL(18, 2)) AS annual_retainer_usd,
        try_cast(nullif(trim(contract_status), '') AS VARCHAR(50)) AS contract_status,

        try_cast(nullif(trim(drs_review_id), '') AS VARCHAR(50)) AS drs_review_id,
        try_cast(nullif(trim(drs_reviewed_by), '') AS VARCHAR(50)) AS drs_reviewed_by,
        try_cast(nullif(trim(drs_original_decision), '') AS VARCHAR(50)) AS drs_original_decision,
        try_cast(nullif(trim(drs_outcome), '') AS VARCHAR(50)) AS drs_outcome,

        try_cast(nullif(trim(sourcefile), '') AS VARCHAR(500)) AS sourcefile,
        load_timestamp,

        ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY load_timestamp DESC NULLS LAST) AS row_num

    FROM raw_source

)

SELECT
    delivery_key,
    delivery_id,
    delivery_timestamp,
    innings_no,
    over_no,
    ball_in_over,
    runs_off_bat,
    extra_type,
    extra_runs,
    total_runs,
    is_wicket,
    dismissal_type,
    ball_speed_kmph,
    non_striker_id,
    shot_type,
    shot_region,
    match_id,
    match_date,
    match_format,
    tournament_name,
    season,
    match_stage,
    toss_decision,
    venue_id,
    venue_name,
    venue_city,
    venue_country,
    batting_team_id,
    batting_team_name,
    bowling_team_id,
    bowling_team_name,
    batsman_id,
    batsman_name,
    batsman_dob,
    batsman_nationality,
    batting_hand,
    batsman_role,
    batsman_email,
    batsman_phone,
    batsman_icc_rating,
    bowler_id,
    bowler_name,
    bowler_nationality,
    bowling_style,
    bowler_icc_rating,
    contract_id,
    contract_grade,
    annual_retainer_usd,
    contract_status,
    drs_review_id,
    drs_reviewed_by,
    drs_original_decision,
    drs_outcome,
    sourcefile,
    load_timestamp
FROM clean
WHERE row_num = 1
