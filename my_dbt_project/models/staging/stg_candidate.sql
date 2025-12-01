{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'CANDIDATES') }}

),

renamed as (
    select
        "CANDIDATE_ID" as id,
        "PRIMARY_SKILL_ID" as primary_skill_id,
        "JOB_FUNCTION_ID" as job_function_id,
        "STAFFING_STATUS" as staffing_status,
        "ENGLISH_LEVEL" as english_level,
        to_timestamp_ntz(try_to_number("_CREATED_MICROS") / 1000000) as created_at,
        to_timestamp_ntz(try_to_number("_UPDATED_MICROS") / 1000000) as updated_at,
        "_OFFSET" as offset
    from source
)

select * from renamed