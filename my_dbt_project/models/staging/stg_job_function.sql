{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'JOB_FUNCTIONS') }}

),

renamed as (

    select
        "JOB_FUNCTION_ID" as id,
        "BASE_NAME" as base_name,
        "CATEGORY" as category,
        "LEVEL" as level,
        "TRACK" as track,
        "SENIORITY_LEVEL" as seniority_level,
        try_to_number("SENIORITY_INDEX") as seniority_index,
        "IS_ACTIVE"::boolean as is_active,
        to_timestamp_ntz(try_to_number("_CREATED_MICROS") / 1000000) as created_at,
        to_timestamp_ntz(try_to_number("_UPDATED_MICROS") / 1000000) as updated_at,
        "_OFFSET" as offset

    from source

)

select * from renamed