-- tests/generic/test_is_unique_per_partition.sql
{% test is_unique_per_partition(model, column_name, partition_column) %}

with validation_errors as (
    select
        {{ partition_column }}
    from {{ model }}
    group by {{ partition_column }}
    having count(distinct {{ column_name }}) > 1
)

select *
from validation_errors

{% endtest %}