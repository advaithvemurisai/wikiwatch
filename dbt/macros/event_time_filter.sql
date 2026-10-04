{#- Partition-pruned filter on an event-time column, as a literal timestamp computed at
    compile time. A literal (not a subquery) guarantees Iceberg partition pruning on both
    Trino and Athena, so every scheduled run reads only recent partitions.

    Incremental run: run start - lookback_hours. First run / full refresh: run start -
    initial_lookback_days. The bound is rounded down to `grain` ('hour' or 'day') so a
    recomputed window or day is always complete before the merge overwrites it. -#}
{% macro event_time_filter(column, grain='hour') -%}
    {{ column }} >= timestamp '{{ event_time_floor(grain) }}'
{%- endmacro %}

{% macro event_time_floor(grain='hour') -%}
    {%- if var('event_time_floor') is not none -%}
        {{- var('event_time_floor') -}}
    {%- else -%}
        {%- if is_incremental() -%}
            {%- set back = modules.datetime.timedelta(hours=var('lookback_hours')) -%}
        {%- else -%}
            {%- set back = modules.datetime.timedelta(days=var('initial_lookback_days')) -%}
        {%- endif -%}
        {%- set bound = run_started_at - back -%}
        {%- if grain == 'day' -%}
            {%- set bound = bound.replace(hour=0, minute=0, second=0, microsecond=0) -%}
        {%- else -%}
            {%- set bound = bound.replace(minute=0, second=0, microsecond=0) -%}
        {%- endif -%}
        {{- bound.strftime('%Y-%m-%d %H:%M:%S') -}}
    {%- endif -%}
{%- endmacro %}

{#- For data tests: only the last test_window_days of data (partition-pruned). -#}
{% macro recent(column) -%}
    {%- set bound = run_started_at - modules.datetime.timedelta(days=var('test_window_days')) -%}
    {{ column }} >= timestamp '{{ bound.strftime('%Y-%m-%d %H:%M:%S') }}'
{%- endmacro %}

{#- Reconciliation cutoff: events processed before this are settled (alerts committed). -#}
{% macro settled_before() -%}
    {%- set in_flight = modules.datetime.timedelta(minutes=var('in_flight_minutes')) -%}
    {%- set cutoff = run_started_at - in_flight -%}
    {{- cutoff.strftime('%Y-%m-%d %H:%M:%S') -}}
{%- endmacro %}
