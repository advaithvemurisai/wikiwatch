{#- Complete 1-minute windows per wiki, built from Silver so they include late events.
    Same definition as the streaming gold.edits_per_min (edit and new events only), so the
    difference between the two tables is exactly what the watermark dropped. -#}
{{ config(
    unique_key=['window_start', 'wiki']
) }}

select
    date_trunc('minute', event_ts) as window_start,
    date_trunc('minute', event_ts) + interval '1' minute as window_end,
    wiki,
    count(*) as edits,
    count_if(is_bot) as bot_edits,
    coalesce(sum(abs(byte_delta)), 0) as bytes_changed
from {{ source('silver', 'wiki_edits') }}
where
    edit_type in ('edit', 'new')
    and {{ event_time_filter('event_ts', 'hour') }}
group by 1, 2, 3
