{{ config(materialized='table', schema='gold') }}

WITH contract_states AS (
    select
        contract_id,
        contract_grade,
        annual_retainer_usd,
        contract_status AS status,
        MIN(delivery_timestamp) AS effective_from,
        MAX(delivery_timestamp) AS latest_delivery_ts
    from {{ ref('silver_cricket_data') }}
    where contract_id is not null
    group by contract_id,contract_grade,annual_retainer_usd,contract_status

),

contract_windows as (

    select
        contract_id,
        contract_grade,
        annual_retainer_usd,
        status,
        effective_from,
        lead(effective_from) OVER (PARTITION BY contract_id order by effective_from) as effective_to,
        case
            when lead(effective_from) OVER (PARTITION BY contract_id ORDER BY effective_from
            ) is null then TRUE
            else FALSE
        end as active_contract
    from contract_states

)

select
    contract_id,
    contract_grade,
    annual_retainer_usd,
    status,
    effective_from,
    effective_to,
    active_contract
from contract_windows
