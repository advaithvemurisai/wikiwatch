{#- Overrides dbt-athena's resolve_table_type (same body) for one case: unit tests.
    Before a unit test runs, dbt creates an empty table from the model's SQL to learn its
    column types. That node carries none of the model's config, so dbt-athena would make it
    a Hive table, and Hive tables on Athena keep timestamps only to the millisecond: the
    create fails on burst_alerts' timestamp(6) columns. Iceberg keeps microseconds, as Gold
    does. Only dbt-athena calls this macro; dbt-trino ignores it. -#}
{% macro resolve_table_type() %}
    {%- if config.get('materialized') == 'unit' -%}
        {{ return('iceberg') }}
    {%- endif -%}
    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
    {%- set table_type = config.get('table_type', default=none) -%}
    {%- if table_type is none and catalog_relation is not none -%}
        {%- set table_type = catalog_relation.table_format -%}
    {%- endif -%}
    {{ return((table_type or 'hive') | lower) }}
{% endmacro %}
