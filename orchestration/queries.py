"""Every query the dashboard export and the freshness check run, in one place.

Rules (CLAUDE.md cost rules, ADR 0008):
- Same SQL on local Trino and on Athena engine v3 (Trino dialect, shared functions only).
- Every scan of a partitioned table carries a literal timestamp or date bound on its
  partition column, rendered here in Python. A literal (never now() or a subquery) is
  guaranteed to prune Iceberg partitions on both engines. Each query declares the tables it
  scans; tests check the bound is present for every one of them.
- Bounds are relative to `now`, the run's logical time, so a re-run renders the same SQL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta

# Partition column of every partitioned table the export reads. Unpartitioned tables
# (the two tiny ref tables) map to None and need no bound.
PARTITION_COLUMNS: dict[str, str | None] = {
    "bronze.wiki_edits_raw": "ingested_at",
    "bronze.wiki_edits_dlq": "ingested_at",
    "silver.wiki_edits": "event_ts",
    "gold.edits_per_min": "window_start",
    "gold.edits_per_min_final": "window_start",
    "gold.watched_page_alerts": "event_ts",
    "gold.burst_alerts": "window_start",
    "gold.bot_ratio_hourly": "hour_start",
    "gold.watchlist_daily_digest": "digest_date",
    "ref.watchlist": None,
    "ref.alert_rules": None,
}

ALERT_WINDOW = timedelta(days=7)
BASELINE_MINUTES_WINDOW = timedelta(hours=6)
HEALTH_WINDOW = timedelta(hours=12)  # sessions shut down after 4 hours
SESSION_ID = re.compile(r"^[A-Za-z0-9-]+$")


@dataclass(frozen=True)
class Query:
    """A named SQL template and the tables it scans (alias -> table)."""

    name: str
    template: str
    scans: dict[str, str] = field(default_factory=dict)

    def render(self, now: datetime, **params: str) -> str:
        """Fill the literal bounds (and any extra parameters) for this run."""
        bounds = {
            "since_7d": _ts(now - ALERT_WINDOW),
            "since_7d_date": (now - ALERT_WINDOW).date().isoformat(),
            "since_6h": _ts(now - BASELINE_MINUTES_WINDOW),
            "since_12h": _ts(now - HEALTH_WINDOW),
        }
        return self.template.format(**bounds, **params)


def _ts(value: datetime) -> str:
    return value.replace(tzinfo=None, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def session_param(session_id: str) -> str:
    """Validate a session ID before it goes into SQL (it comes from the broker)."""
    if not SESSION_ID.fullmatch(session_id):
        raise ValueError("unexpected characters in session ID")
    return session_id


# ---------------------------------------------------------------- alerts.json

ALERTS = Query(
    "alerts",
    """
    select * from (
        select a.alert_id, a.rule_id, r.rule_name, a.severity, a.wiki, a.title, a.category,
               a.owner_team, a.edit_type, a.log_action, a.editor_type, a.byte_delta,
               cast(null as bigint) as edits_in_window, a.event_ts, a.detected_at
        from gold.watched_page_alerts as a
        inner join ref.alert_rules as r on a.rule_id = r.rule_id
        where a.event_ts >= timestamp '{since_7d}'
        union all
        select b.alert_id, b.rule_id, r.rule_name, b.severity, b.wiki, b.title, b.category,
               b.owner_team, cast(null as varchar), cast(null as varchar),
               cast(null as varchar), cast(null as bigint), b.edits, b.window_start,
               b.detected_at
        from gold.burst_alerts as b
        inner join ref.alert_rules as r on b.rule_id = r.rule_id
        where b.window_start >= timestamp '{since_7d}'
    ) as alerts
    order by case severity when 'high' then 0 when 'medium' then 1 else 2 end, event_ts desc
    limit 500
    """,
    scans={"a": "gold.watched_page_alerts", "b": "gold.burst_alerts", "r": "ref.alert_rules"},
)

DIGEST = Query(
    "digest",
    """
    select digest_date, wiki, title, category, edits, log_events, net_bytes,
           registered_edits, unregistered_edits, bot_edits,
           r1_alerts, r2_alerts, r3_alerts, r4_alerts, r5_alerts
    from gold.watchlist_daily_digest
    where digest_date >= date '{since_7d_date}'
    order by digest_date desc, edits desc
    limit 500
    """,
    scans={"": "gold.watchlist_daily_digest"},
)

# ---------------------------------------------------------------- baseline.json

EDITS_PER_MIN_FINAL = Query(
    "edits_per_min_final",
    """
    select window_start as minute, sum(edits) as edits
    from gold.edits_per_min_final
    where window_start >= timestamp '{since_6h}'
    group by 1
    """,
    scans={"": "gold.edits_per_min_final"},
)

EDITS_PER_MIN_REALTIME = Query(
    "edits_per_min_realtime",
    """
    select window_start as minute, sum(edits) as edits
    from gold.edits_per_min
    where window_start >= timestamp '{since_6h}'
    group by 1
    """,
    scans={"": "gold.edits_per_min"},
)

BOT_SHARE_HOURLY = Query(
    "bot_share_hourly",
    """
    select hour_start as hour, sum(edits) as edits, sum(bot_edits) as bot_edits
    from gold.bot_ratio_hourly
    where hour_start >= timestamp '{since_7d}'
    group by 1
    order by 1
    """,
    scans={"": "gold.bot_ratio_hourly"},
)

RECORDED_HOURS = Query(
    "recorded_hours",
    """
    select distinct date_trunc('hour', window_start) as hour
    from gold.edits_per_min_final
    where window_start >= timestamp '{since_7d}'
    order by 1
    """,
    scans={"": "gold.edits_per_min_final"},
)

PAGE_ACTIVITY = Query(
    "page_activity",
    """
    select w.title, w.category, date_trunc('hour', e.event_ts) as hour, count(*) as edits
    from silver.wiki_edits as e
    inner join ref.watchlist as w on e.wiki = w.wiki and e.title = w.title
    where e.namespace = 0 and e.edit_type in ('edit', 'new')
      and e.event_ts >= timestamp '{since_7d}'
    group by 1, 2, 3
    order by 3 desc, 4 desc
    limit 3000
    """,
    scans={"e": "silver.wiki_edits", "w": "ref.watchlist"},
)

WATCHLIST = Query(
    "watchlist",
    "select title, category from ref.watchlist order by title",
    scans={"": "ref.watchlist"},
)

# ---------------------------------------------------------------- health.json

FRESHNESS = Query(
    "freshness",
    """
    select
        (select max(ingested_at) from bronze.wiki_edits_raw
         where ingested_at >= timestamp '{since_12h}') as bronze,
        (select max(event_ts) from silver.wiki_edits
         where event_ts >= timestamp '{since_12h}') as silver,
        (select max(window_start) from gold.edits_per_min
         where window_start >= timestamp '{since_12h}') as gold
    """,
    scans={
        "b": "bronze.wiki_edits_raw",
        "s": "silver.wiki_edits",
        "g": "gold.edits_per_min",
    },
)

FUNNEL = Query(
    "funnel",
    """
    select
        (select count(*) from bronze.wiki_edits_raw
         where session_id = '{session_id}' and ingested_at >= timestamp '{since_12h}') as bronze,
        (select count(distinct meta_id) from bronze.wiki_edits_raw
         where session_id = '{session_id}' and ingested_at >= timestamp '{since_12h}')
            as silver_unique,
        (select count(*) from bronze.wiki_edits_dlq
         where session_id = '{session_id}' and ingested_at >= timestamp '{since_12h}') as dlq
    """,
    scans={"b": "bronze.wiki_edits_raw", "d": "bronze.wiki_edits_dlq"},
)

# Detection latency for alerts that arrived on time (late arrivals after a resume would
# measure the outage, not the pipeline).
DETECTION = Query(
    "detection",
    """
    select count(*) as samples,
           approx_percentile(date_diff('millisecond', event_ts, detected_at) / 1000.0, 0.5)
               as p50,
           approx_percentile(date_diff('millisecond', event_ts, detected_at) / 1000.0, 0.95)
               as p95
    from gold.watched_page_alerts
    where event_ts >= timestamp '{since_7d}'
      and ingested_at < event_ts + interval '10' minute
    """,
    scans={"": "gold.watched_page_alerts"},
)

EXPORT_QUERIES = (
    ALERTS,
    DIGEST,
    EDITS_PER_MIN_FINAL,
    EDITS_PER_MIN_REALTIME,
    BOT_SHARE_HOURLY,
    RECORDED_HOURS,
    PAGE_ACTIVITY,
    WATCHLIST,
)
HEALTH_QUERIES = (FRESHNESS, FUNNEL, DETECTION)
ALL_QUERIES = EXPORT_QUERIES + HEALTH_QUERIES
