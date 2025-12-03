{{ config(
    materialized='table'
) }}

select
    candidate_id,
    primary_skill_id,
    staffing_status,
    english_level,
    job_function_id,
    updated_at as last_updated_at

from {{ ref('stg_latest_candidate') }}