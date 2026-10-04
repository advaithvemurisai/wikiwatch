{#- Hourly bot share per wiki: the baseline for "is this activity normal?" (BR4). -#}
{{ config(
    unique_key=['hour_start', 'wiki']
) }}

select
    date_trunc('hour', event_ts) as hour_start,
    wiki,
    count(*) as edits,
    count_if(is_bot) as bot_edits,
    count_if(not coalesce(is_bot, false)) as human_edits,
    cast(count_if(is_bot) as double) / count(*) as bot_share
from {{ source('silver', 'wiki_edits') }}
where
    edit_type in ('edit', 'new')
    and {{ event_time_filter('event_ts', 'hour') }}
group by 1, 2
