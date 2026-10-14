{#
    Restricts bronze reads to a fixed window outside prod.

    The filter compares the partition column directly with a literal of the
    same type, so BigQuery can prune partitions. Wrapping the column in a
    function (e.g. date(col)) can prevent pruning and scan the whole table.
#}
{% macro limit_dev_data(column, column_type='datetime') -%}
    {%- if target.name != 'prod' -%}
        where {{ column }} >= {{ column_type }}('{{ var("dev_window_start") }}')
          and {{ column }} < {{ column_type }}('{{ var("dev_window_end") }}')
    {%- endif -%}
{%- endmacro %}
