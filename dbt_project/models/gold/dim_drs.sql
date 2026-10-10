{{ config(materialized='table',schema='gold') }}

select
    drs_review_id,
    drs_reviewed_by AS reviewed_by,
    drs_original_decision AS original_decision,
    drs_outcome AS outcome
from {{ ref('silver_cricket_data') }}
where drs_review_id is not null
