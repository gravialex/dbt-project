{% macro staging_model(source_name, natural_key, incremental_filter=none) %}

{#-
    This macro builds a historical (SCD Type 2) staging model.

    ARGS:
    - source_name (string): The name of the raw table.
    - natural_key (string): The column name that uniquely identifies an entity.
    - incremental_filter (string): An optional SQL filter to be applied for incremental runs.
-#}

{%- set config_query -%}
    select
        "raw_column_name",
        "target_column_name",
        "target_data_type"
    from {{ ref('staging_config') }}
    where "raw_table_name" = '{{ source_name | upper }}'
    order by "target_order_num" asc
{%- endset -%}

{%- set config_results = run_query(config_query) -%}

{%- if execute -%}
    {%- set columns = config_results.rows -%}
{%- else -%}
    {%- set columns = [] -%}
{%- endif -%}


with source_data as (

    select * from {{ source('raw', source_name | lower) }}

    {% if incremental_filter %}
    where {{ incremental_filter }}
    {% endif %}

),

renamed_and_typed as (

    select
        {%- if columns | length == 0 %}
        1 as _dummy_col_for_parser
        {%- else %}
            {% for column in columns -%}
                {%- set raw_col = column.raw_column_name -%}
                {%- set target_col = column.target_column_name -%}
                {%- set target_type = column.target_data_type -%}

                {%- if target_type == 'TIMESTAMP_NTZ' and raw_col.endswith('_MICROS') -%}
                to_timestamp_ntz(try_cast({{ raw_col }} as bigint) / 1000000) as {{ target_col }}
                {%- elif target_type == 'BOOLEAN' -%}
                iff(upper({{ raw_col }}) in ('TRUE', '1'), true, false) as {{ target_col }}
                {%- elif target_type == 'NUMBER' -%}
                try_cast({{ raw_col }} as number) as {{ target_col }}
                {%- else -%}
                {{ raw_col }} as {{ target_col }}
                {%- endif -%}
                {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        {%- endif %}
    from source_data

),

windowed as (

    select
        *,
        lead(updated_at, 1) over (partition by {{ natural_key }} order by updated_at) as _row_valid_to
    from renamed_and_typed

),

historical as (

    select
        *,
        updated_at as row_valid_from,
        iff(_row_valid_to is null, true, false) as row_is_active
    from windowed

),

final as (

    select
        {# THIS IS THE CORRECTED LOGIC #}
        {% for column in columns -%}
        {{ column.target_column_name }},
        {% endfor %}

        row_valid_from,
        coalesce(_row_valid_to, '9999-12-31 23:59:59.999'::timestamp_ntz) as row_valid_to,
        row_is_active

    from historical
)

select * from final

{% endmacro %}