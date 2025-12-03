{{ config(
    materialized='incremental',
    unique_key=['job_function_id', 'row_valid_from']
) }}

{{ staging_model(
    source_name='job_functions',
    natural_key='job_function_id'
) }}

{% if is_incremental() %}

  -- this filter can be adapted based on the replication strategy
  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}