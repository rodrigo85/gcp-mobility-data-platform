{#
    prod: the dataset is exactly the requested name (silver, gold, snapshots).
    dev/ci: everything lives under the target prefix (dev_rodrigo_gold, ci_pr_42_gold),
    so nobody overwrites prod and each PR gets its own environment.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
