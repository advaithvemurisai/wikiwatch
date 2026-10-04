{#- Fails when the given columns together are not unique (no external package needed). -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as rows_per_key
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
