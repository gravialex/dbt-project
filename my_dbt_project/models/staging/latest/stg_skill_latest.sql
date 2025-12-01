{{ config(materialized='table') }}

with source as (

    select * from {{ ref('stg_skill') }}

),

latest as (

    select
        *,
        row_number() over (partition by id order by updated_at desc) as rn
    from source

)

select
    id,
    parent_skill_id,
    skill_name,
    skill_type,
    skill_url,
    key_skill_reason,
    is_active,
    is_primary,
    is_key,
    created_at,
    updated_at
from latest
where rn = 1