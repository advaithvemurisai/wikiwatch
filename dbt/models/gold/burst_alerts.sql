{#- R5 Edit burst: at least min_edits edits (5) on one watched article within one
    window_minutes (10) window. Windows are fixed and aligned to the clock (:00, :10, ...),
    so the alert is deterministic: alert_id = sha256("R5:<wiki>:<title>:<window_start>").
    Bots count toward bursts (docs/v1.md). Trade-off: a burst split across a window
    boundary (3 + 2 edits) is not flagged; sliding windows are a v2 option.
    window_minutes must divide 60 (checked by the ref loader). -#}
{{ config(
    unique_key='alert_id',
    merge_exclude_columns=['alert_id', 'detected_at']
) }}

with rule as (
    select
        severity,
        min_edits,
        window_minutes
    from {{ source('ref', 'alert_rules') }}
    where rule_id = 'R5' and enabled
),

watched_edits as (
    select
        e.wiki,
        e.title,
        e.event_ts,
        w.category,
        w.owner_team
    from {{ source('silver', 'wiki_edits') }} as e
    inner join {{ source('ref', 'watchlist') }} as w
        on e.wiki = w.wiki and e.title = w.title
    where
        e.namespace = 0
        and e.edit_type in ('edit', 'new')
        and {{ event_time_filter('e.event_ts', 'hour') }}
),

windowed as (
    select
        we.*,
        r.severity,
        r.min_edits,
        r.window_minutes,
        date_add(
            'minute',
            cast(floor(minute(we.event_ts) / r.window_minutes) * r.window_minutes as bigint),
            date_trunc('hour', we.event_ts)
        ) as window_start
    from watched_edits as we
    cross join rule as r
),

bursts as (
    select
        wiki,
        title,
        category,
        owner_team,
        severity,
        window_start,
        date_add('minute', max(window_minutes), window_start) as window_end,
        count(*) as edits,
        min(event_ts) as first_edit_ts,
        max(event_ts) as last_edit_ts
    from windowed
    group by wiki, title, category, owner_team, severity, window_start
    having count(*) >= max(min_edits)
)

select
    'R5' as rule_id,
    severity,
    wiki,
    title,
    category,
    owner_team,
    window_start,
    window_end,
    edits,
    first_edit_ts,
    last_edit_ts,
    cast(current_timestamp at time zone 'UTC' as timestamp(6)) as detected_at,
    lower(to_hex(sha256(to_utf8(
        concat_ws(':', 'R5', wiki, title, format_datetime(window_start, 'yyyy-MM-dd''T''HH:mm:ss'))
    )))) as alert_id
from bursts
