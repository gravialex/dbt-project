{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with source as (

    select * from {{ source('raw', 'INTERVIEWS') }}

),

renamed as (

    select
        "ID" as id,
        "CANDIDATE_ID" as candidate_id,
        "INTERVIEWER_ID" as interviewer_id,
        "CANDIDATE_TYPE" as candidate_type,
        "STATUS" as interview_status,
        "LOCATION" as location,
        "RUN_TYPE" as run_type,
        "TYPE" as interview_type,
        "MEDIA_STATUS" as media_status,
        "INVITE_ANSWER_STATUS" as invite_answer_status,
        "LOGGED"::boolean as is_logged,
        "MEDIA_AVAILABLE"::boolean as is_media_available,
        to_timestamp_ntz(try_to_number("_CREATED_MICROS") / 1000000) as created_at,
        to_timestamp_ntz(try_to_number("_UPDATED_MICROS") / 1000000) as updated_at,
        "_OFFSET" as offset

    from source
    
    {% if is_incremental() %}

        where to_timestamp_ntz(try_to_number("_UPDATED_MICROS") / 1000000) > (select coalesce(max(updated_at), to_timestamp_ntz('1900-01-01')) from {{ this }})

    {% endif %}

)

select * from renamed