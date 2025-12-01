{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'SKILLS') }}

),

renamed as (

    select
        "ID" as id,
        "PARENT_ID" as parent_skill_id,
        "NAME" as skill_name,
        "TYPE" as skill_type,
        "URL" as skill_url,
        "IS_KEY_REASON" as key_skill_reason,
        "IS_ACTIVE"::boolean as is_active,
        "IS_PRIMARY"::boolean as is_primary,
        "IS_KEY"::boolean as is_key,
        to_timestamp_ntz(try_to_number("_CREATED_MICROS") / 1000000) as created_at,
        to_timestamp_ntz(try_to_number("_UPDATED_MICROS") / 1000000) as updated_at,
        "_OFFSET" as offset

    from source

)

select * from renamed