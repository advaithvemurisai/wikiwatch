{#- Daily summary per watched page for the communications lead (BR5): activity,
    net bytes, editor-type mix and alert counts per rule. Days are UTC. -#}
{{ config(
    unique_key=['digest_date', 'wiki', 'title']
) }}

with page_events as (
    select
        cast(e.event_ts as date) as digest_date,
        e.wiki,
        e.title,
        w.category,
        w.owner_team,
        e.edit_type,
        e.editor_type,
        e.byte_delta
    from {{ source('silver', 'wiki_edits') }} as e
    inner join {{ source('ref', 'watchlist') }} as w
        on e.wiki = w.wiki and e.title = w.title
    where e.namespace = 0 and {{ event_time_filter('e.event_ts', 'day') }}
),

activity as (
    select
        digest_date,
        wiki,
        title,
        category,
        owner_team,
        count_if(edit_type in ('edit', 'new')) as edits,
        count_if(edit_type = 'log') as log_events,
        coalesce(sum(byte_delta), 0) as net_bytes,
        count_if(edit_type in ('edit', 'new') and editor_type = 'registered') as registered_edits,
        count_if(edit_type in ('edit', 'new') and editor_type = 'unregistered')
            as unregistered_edits,
        count_if(edit_type in ('edit', 'new') and editor_type = 'bot') as bot_edits
    from page_events
    group by 1, 2, 3, 4, 5
),

stream_alerts as (
    select
        cast(event_ts as date) as digest_date,
        wiki,
        title,
        count_if(rule_id = 'R1') as r1_alerts,
        count_if(rule_id = 'R2') as r2_alerts,
        count_if(rule_id = 'R3') as r3_alerts,
        count_if(rule_id = 'R4') as r4_alerts
    from {{ source('gold_stream', 'watched_page_alerts') }}
    where {{ event_time_filter('event_ts', 'day') }}
    group by 1, 2, 3
),

burst_counts as (
    select
        cast(window_start as date) as digest_date,
        wiki,
        title,
        count(*) as r5_alerts
    from {{ ref('burst_alerts') }}
    where {{ event_time_filter('window_start', 'day') }}
    group by 1, 2, 3
)

select
    a.digest_date,
    a.wiki,
    a.title,
    a.category,
    a.owner_team,
    a.edits,
    a.log_events,
    a.net_bytes,
    a.registered_edits,
    a.unregistered_edits,
    a.bot_edits,
    coalesce(s.r1_alerts, 0) as r1_alerts,
    coalesce(s.r2_alerts, 0) as r2_alerts,
    coalesce(s.r3_alerts, 0) as r3_alerts,
    coalesce(s.r4_alerts, 0) as r4_alerts,
    coalesce(b.r5_alerts, 0) as r5_alerts
from activity as a
left join stream_alerts as s
    on a.digest_date = s.digest_date and a.wiki = s.wiki and a.title = s.title
left join burst_counts as b
    on a.digest_date = b.digest_date and a.wiki = b.wiki and a.title = b.title
