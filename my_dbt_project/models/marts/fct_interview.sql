{{ config(
    materialized='table'
) }}

with interviews_pivoted as (

    select * from {{ ref('int_interview_status_pivoted') }}

),

-- Join to historical staging tables to get the state of dimensions at the time of interview creation
point_in_time_joins as (

    select
        interviews_pivoted.interview_id,
        interviews_pivoted.created_at,
        interviews_pivoted.interview_type,

        -- Timestamps for each status
        interviews_pivoted.scheduled_at,
        interviews_pivoted.in_progress_at,
        interviews_pivoted.pending_feedback_at,
        interviews_pivoted.completed_at,
        interviews_pivoted.passed_at,
        interviews_pivoted.failed_at,
        interviews_pivoted.cancelled_at,

        -- Candidate attributes at the time of interview creation
        candidate_hist.candidate_id,
        candidate_hist.primary_skill_id as candidate_primary_skill_id,
        candidate_hist.english_level as candidate_english_level,
        candidate_hist.job_function_id as candidate_job_function_id,

        -- Interviewer attributes at the time of interview creation
        employee_hist.employee_id as interviewer_id,
        employee_hist.job_function_id as interviewer_job_function_id,
        employee_hist.org_category as interviewer_org_category

    from interviews_pivoted
    left join {{ ref('stg_candidate') }} as candidate_hist
        on interviews_pivoted.candidate_id = candidate_hist.candidate_id
        and interviews_pivoted.created_at between candidate_hist.row_valid_from and candidate_hist.row_valid_to

    left join {{ ref('stg_employee') }} as employee_hist
        on interviews_pivoted.interviewer_id = employee_hist.employee_id
        and interviews_pivoted.created_at between employee_hist.row_valid_from and employee_hist.row_valid_to

),

final as (

    select
        -- Primary Key
        interview_id,

        -- Foreign Keys
        candidate_id,
        interviewer_id,
        candidate_primary_skill_id,
        candidate_job_function_id,
        interviewer_job_function_id,

        -- Degenerate Dimensions (Interview Attributes)
        interview_type,
        candidate_english_level,
        interviewer_org_category,

        -- Timestamps
        created_at,
        scheduled_at,
        in_progress_at,
        pending_feedback_at,
        completed_at,
        passed_at,
        failed_at,
        cancelled_at,

        -- Calculated Measures (Durations in Seconds)
        case
            when interview_type = 'online' and in_progress_at is not null and pending_feedback_at is not null
            then datediff('second', in_progress_at, pending_feedback_at)
            else null
        end as interview_duration_seconds,

        case
            when pending_feedback_at is not null and completed_at is not null
            then datediff('second', pending_feedback_at, completed_at)
            else null
        end as feedback_delay_seconds

    from point_in_time_joins
)

select * from final