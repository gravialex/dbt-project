{{ config(
    materialized='incremental',
    unique_key=['interview_id', 'row_valid_from'],
    on_schema_change="sync_all_columns"
) }}

with source_data as (

    select * from {{ source('raw', 'interviews') }}

    {% if is_incremental() %}

    -- Filter on the raw source column, converting the max timestamp from the target table back to microseconds
    where _UPDATED_MICROS > (select extract(epoch_second, max(updated_at)) * 1000000 from {{ this }})

    {% endif %}

),

renamed_and_typed as (

    select
        -- Manually rename and cast each column from the source
        _OFFSET as load_offset,
        ID as interview_id,
        CANDIDATE_TYPE as candidate_type,
        CANDIDATE_ID as candidate_id,
        STATUS as interview_status,
        INTERVIEWER_ID as interviewer_id,
        LOCATION as location,
        iff(upper(LOGGED) in ('TRUE', '1'), true, false) as is_logged,
        iff(upper(MEDIA_AVAILABLE) in ('TRUE', '1'), true, false) as is_media_available,
        RUN_TYPE as run_type,
        TYPE as interview_type,
        MEDIA_STATUS as media_status,
        INVITE_ANSWER_STATUS as invite_answer_status,
        to_timestamp_ntz(try_cast(_CREATED_MICROS as bigint) / 1000000) as created_at,
        to_timestamp_ntz(try_cast(_UPDATED_MICROS as bigint) / 1000000) as updated_at

    from source_data

),

windowed as (

    select
        *,
        lead(updated_at, 1) over (partition by interview_id order by updated_at) as _row_valid_to
    from renamed_and_typed

),

final as (

    select
        -- Select the final list of columns explicitly
        load_offset,
        interview_id,
        candidate_type,
        candidate_id,
        interview_status,
        interviewer_id,
        location,
        is_logged,
        is_media_available,
        run_type,
        interview_type,
        media_status,
        invite_answer_status,
        created_at,
        updated_at,

        -- Generate the historical tracking columns
        updated_at as row_valid_from,
        coalesce(_row_valid_to, '9999-12-31 23:59:59.999'::timestamp_ntz) as row_valid_to,
        iff(_row_valid_to is null, true, false) as row_is_active

    from windowed
)

select * from final