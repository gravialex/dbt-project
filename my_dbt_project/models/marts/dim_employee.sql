{{ config(
    materialized='table'
) }}

select
    employee_id,
    job_function_id,
    primary_skill_id,
    production_category,
    employment_status,
    org_category,
    org_category_type,
    is_active,
    work_started_at,
    updated_at as last_updated_at

from {{ ref('stg_latest_employee') }}