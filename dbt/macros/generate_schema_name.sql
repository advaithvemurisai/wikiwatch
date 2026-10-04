{#- Write models to the schema named in config (gold), not dbt's default "<target>_gold":
    Spark and dbt share the same gold database in Glue and in the local catalog. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
