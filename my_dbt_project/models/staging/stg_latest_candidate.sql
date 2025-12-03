{{ config(
    materialized='view'
) }}

{#
    This model creates a clean, up-to-date view of candidates
    by selecting only the currently active records from the historical table.
#}

{{ latest_staging_model(
    staging_ref=ref('stg_candidate')
) }}