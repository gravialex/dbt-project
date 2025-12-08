select
    interview_id,
    interview_duration_seconds
from {{ ref('fct_interview') }}
where interview_duration_seconds < 0