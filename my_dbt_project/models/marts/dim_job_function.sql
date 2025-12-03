{{ config(
    materialized='table'
) }}

select
    job_function_id,
    job_function_base_name,
    job_function_category,
    is_active,
    job_level,
    career_track,
    seniority_level,
    seniority_index

from {{ ref('stg_latest_job_function') }}