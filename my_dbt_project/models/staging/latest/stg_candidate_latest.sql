{{ config(materialized='table') }}

with source as (

    select * from {{ ref('stg_candidate') }}

),

latest as (

    select
        *,
        row_number() over (partition by id order by updated_at desc) as rn
    from source

)

select
    id,
    primary_skill_id,
    job_function_id,
    staffing_status,
    english_level,
    created_at,
    updated_at
from latest
where rn = 1