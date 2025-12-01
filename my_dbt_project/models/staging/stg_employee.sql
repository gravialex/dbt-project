{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'EMPLOYEES') }}

),

renamed as (
    select
        "EMPLOYEE_ID" as id,
        "JOB_FUNCTION_ID" as job_function_id,
        "PRIMARY_SKILL_ID" as primary_skill_id,
        "PRODUCTION_CATEGORY" as production_category,
        "EMPLOYMENT_STATUS" as employment_status,
        "ORG_CATEGORY" as org_category,
        "ORG_CATEGORY_TYPE" as org_category_type,
        "IS_ACTIVE"::boolean as is_active,
        to_timestamp_ntz(try_to_number("WORK_START_MICROS") / 1000000) as work_start_date,
        to_timestamp_ntz(try_to_number("WORK_END_MICROS") / 1000000) as work_end_date,
        to_timestamp_ntz(try_to_number("_CREATED_MICROS") / 1000000) as created_at,
        to_timestamp_ntz(try_to_number("_UPDATED_MICROS") / 1000000) as updated_at,
        "_OFFSET" as offset

    from source

)

select * from renamed