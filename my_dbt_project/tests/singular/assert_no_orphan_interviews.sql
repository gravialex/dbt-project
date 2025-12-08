select
    interview_id
from {{ ref('fct_interview') }}
where candidate_id is null or interviewer_id is null