{{ config(
    materialized='table'
) }}

select
    skill_id,
    is_active,
    skill_type,
    skill_name,
    parent_skill_id

from {{ ref('stg_latest_skill') }}