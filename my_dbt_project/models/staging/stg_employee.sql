{{ config(
    materialized='incremental',
    unique_key=['employee_id', 'row_valid_from']
) }}

{{ staging_model(
    source_name='employees',
    natural_key='employee_id'
) }}

{% if is_incremental() %}

  -- this filter can be adapted based on the replication strategy
  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}