{{ config(
    materialized='incremental',
    unique_key=['candidate_id', 'row_valid_from']
) }}

{{ staging_model(
    source_name='candidates',
    natural_key='candidate_id'
) }}

{% if is_incremental() %}

  -- this filter can be adapted based on the replication strategy
  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}