{% macro latest_staging_model(staging_ref) %}

{#-
    This macro selects the latest, active records from a historical staging model.

    ARGS:
    - staging_ref (ref): A dbt ref to the historical model (e.g., ref('stg_interviews_historical')).
-#}

with staging_model as (

    select * from {{ staging_ref }}

)

select
    * exclude (row_valid_from, row_valid_to, row_is_active)
from staging_model
where row_is_active = true

{% endmacro %}
