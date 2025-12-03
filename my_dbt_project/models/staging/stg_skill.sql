{{ config(
    materialized='incremental',
    unique_key=['skill_id', 'row_valid_from']
) }}

{{ staging_model(
    source_name='skills',
    natural_key='skill_id'
) }}

{% if is_incremental() %}

  -- this filter can be adapted based on the replication strategy
  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}