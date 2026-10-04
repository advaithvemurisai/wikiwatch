{#- Reconciliation (v1 acceptance: 100% of watched-page edits that match a rule have an
    alert). R1 to R4 are re-implemented here in SQL, independently of the Spark code, and
    every match must have a row in gold.watched_page_alerts. Returns the unmatched edits.
    Only recent, settled data is checked: events processed in the last in_flight_minutes
    may not have reached the alerts query yet. -#}

with rules as (
    select
        bool_or(rule_id = 'R1' and enabled) as r1_enabled,
        bool_or(rule_id = 'R2' and enabled) as r2_enabled,
        bool_or(rule_id = 'R3' and enabled) as r3_enabled,
        bool_or(rule_id = 'R4' and enabled) as r4_enabled,
        max(case when rule_id = 'R2' then min_removed_bytes end) as r2_min_bytes,
        max(case when rule_id = 'R2' then min_removed_fraction end) as r2_min_fraction
    from {{ source('ref', 'alert_rules') }}
),

watched as (
    select
        e.meta_id,
        e.edit_type,
        e.log_type,
        e.log_action,
        e.editor_type,
        e.byte_delta,
        e.length_old,
        e.length_new
    from {{ source('silver', 'wiki_edits') }} as e
    inner join {{ source('ref', 'watchlist') }} as w
        on e.wiki = w.wiki and e.title = w.title
    where
        e.namespace = 0
        and {{ recent('e.event_ts') }}
        and e.processed_at < timestamp '{{ settled_before() }}'
),

matches as (
    select
        w.meta_id,
        r.r1_enabled
        and w.edit_type = 'log'
        and w.log_type in ('delete', 'move')
        and w.log_action in ('delete', 'delete_redir', 'move', 'move_redir') as r1,
        r.r2_enabled
        and w.edit_type = 'edit'
        and w.editor_type != 'bot'
        and (
            w.byte_delta <= -r.r2_min_bytes
            or (
                w.length_old > 0
                and cast(w.length_old - w.length_new as decimal(20, 4))
                > r.r2_min_fraction * w.length_old
            )
        ) as r2,
        r.r3_enabled
        and w.edit_type in ('edit', 'new')
        and w.editor_type = 'unregistered' as r3,
        r.r4_enabled
        and w.edit_type = 'log'
        and w.log_type = 'protect' as r4
    from watched as w
    cross join rules as r
),

expected as (
    select
        meta_id,
        'R1' as rule_id
    from matches
    where r1
    union all
    select
        meta_id,
        'R2' as rule_id
    from matches
    where r2
    union all
    select
        meta_id,
        'R3' as rule_id
    from matches
    where r3
    union all
    select
        meta_id,
        'R4' as rule_id
    from matches
    where r4
)

select
    x.meta_id,
    x.rule_id
from expected as x
left join {{ source('gold_stream', 'watched_page_alerts') }} as a
    on
        x.meta_id = a.meta_id
        and x.rule_id = a.rule_id
        and {{ recent('a.event_ts') }}
where a.alert_id is null
