{#
    Airport code (EWR, JFK, LGA) for a TLC zone id column, null elsewhere.
    The mapping lives in the airport_zones var (dbt_project.yml).
#}
{% macro airport_code(zone_id_column) %}
    case {{ zone_id_column }}
        {%- for zone_id, code in var('airport_zones').items() %}
        when {{ zone_id }} then '{{ code }}'
        {%- endfor %}
    end
{%- endmacro %}
