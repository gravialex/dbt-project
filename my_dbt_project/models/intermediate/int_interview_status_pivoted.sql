{{ config(
    materialized='ephemeral'
) }}

{#
    This model pivots the historical interview statuses.
    The grain is one row per interview_id, with timestamps for each status.
#}
select
    interview_id,
    min(candidate_id) as candidate_id,
    min(interviewer_id) as interviewer_id,
    min(interview_type) as interview_type,
    min(created_at) as created_at,

    -- Pivot each status into its own timestamp column
    max(case when interview_status = 'scheduled' then updated_at end) as scheduled_at,
    max(case when interview_status = 'in_progress' then updated_at end) as in_progress_at,
    max(case when interview_status = 'pending_feedback' then updated_at end) as pending_feedback_at,
    max(case when interview_status = 'completed' then updated_at end) as completed_at,
    max(case when interview_status = 'passed' then updated_at end) as passed_at,
    max(case when interview_status = 'failed' then updated_at end) as failed_at,
    max(case when interview_status = 'cancelled' then updated_at end) as cancelled_at

from {{ ref('stg_interview') }}
group by 1