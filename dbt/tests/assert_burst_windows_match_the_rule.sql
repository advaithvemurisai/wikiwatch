{#- Every R5 alert covers exactly the rule's window: window_end = window_start plus
    window_minutes. Runs on real data on both engines; the unit tests leave timestamps
    out of their expected rows (see models/gold/schema.yml). Returns the bad alerts. -#}

with rule as (
    select max(window_minutes) as window_minutes
    from {{ source('ref', 'alert_rules') }}
    where rule_id = 'R5'
)

select
    b.alert_id,
    b.window_start,
    b.window_end
from {{ ref('burst_alerts') }} as b
cross join rule as r
where
    {{ recent('b.window_start') }}
    and b.window_end != date_add('minute', r.window_minutes, b.window_start)
