"""Build the end-to-end fixture from a raw live recording (run once; output is committed).

Input: a JSONL recording made with record_fixture.py (git-ignored, under tmp/).
Output, in tests/e2e/fixtures/:
  e2e_phase1.jsonl.gz  ~5,000 recorded events + injected duplicates, one malformed event
                       and scripted edits on 3 watched pages
  e2e_phase2.jsonl.gz  late events (behind the watermark) + one event that moves the
                       watermark past every fixture window
  e2e_expected.json    exact expected alert IDs and row counts

Privacy (CLAUDE.md, BR8): events on watched pages are dropped (only scripted edits touch
them), user names become stable pseudonyms (Editor-..., Bot-..., synthetic temporary
accounts), free-text and log parameter fields are removed, and any line that still looks
like it contains an IP address is dropped.

Usage: python scripts/build_e2e_fixture.py --raw tmp/e2e_raw.jsonl
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import secrets
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from wikiwatch_producer.privacy import contains_ip

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "tests/e2e/fixtures"
BASE_EVENTS = 5_000
DUPLICATE_EVERY = 100  # 50 duplicates, as a producer resume would re-send them
LATE_EVENTS = 20
DROP_FIELDS = ("comment", "parsedcomment", "log_params", "log_action_comment", "notify_url")
NAMESPACE = uuid.UUID("6f1c2b1e-9b7d-4b52-8d0a-3a8f1f6b5c11")  # fixed: reproducible IDs


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse(dt: str) -> datetime:
    return datetime.fromisoformat(dt.replace("Z", "+00:00"))


class Pseudonyms:
    """Stable per-run pseudonyms. The salt is random and never stored: not reversible."""

    def __init__(self) -> None:
        self.salt = secrets.token_bytes(16)
        self.temp: dict[str, str] = {}

    def __call__(self, user: str, bot: bool) -> str:
        if user.startswith("~"):  # temporary account: keep the format, synthetic digits
            self.temp.setdefault(user, f"~2026-{90000 + len(self.temp):05d}-01")
            return self.temp[user]
        digest = hashlib.sha256(self.salt + user.encode()).hexdigest()[:10]
        return f"{'Bot' if bot else 'Editor'}-{digest}"


def scripted_event(meta_id: str, ts: datetime, title: str, **fields) -> dict:
    event = {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {"id": meta_id, "dt": iso(ts), "domain": "en.wikipedia.org"},
        "type": "edit",
        "namespace": 0,
        "title": title,
        "wiki": "enwiki",
        "user": "Editor-scripted",
        "bot": False,
        "length": {"old": 10_000, "new": 10_040},
    }
    event.update(fields)
    if event.get("length") is None:
        event.pop("length", None)  # log events carry no length (null would fail the schema)
    return event


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", type=Path, required=True)
    args = parser.parse_args()

    with (ROOT / "dbt/seeds/watchlist.csv").open() as f:
        watched = {(r["wiki"], r["title"]) for r in csv.DictReader(f)}
    pseudonym = Pseudonyms()
    base: list[dict] = []
    for line in args.raw.read_text().splitlines():
        event = json.loads(line)
        if (event.get("wiki"), event.get("title")) in watched:
            continue
        for field in DROP_FIELDS:
            event.pop(field, None)
        if "user" in event:
            event["user"] = pseudonym(event["user"], bool(event.get("bot")))
        if contains_ip(json.dumps(event, ensure_ascii=False)):
            continue
        base.append(event)
        if len(base) == BASE_EVENTS:
            break
    if len(base) < BASE_EVENTS:
        print(f"only {len(base)} usable events; record more")
        return 1

    times = sorted(parse(e["meta"]["dt"]) for e in base)
    start, end = times[0], times[-1]
    scripted_at = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
    r5_window = start.replace(minute=start.minute - start.minute % 10, second=0, microsecond=0)
    r5_window += timedelta(minutes=10)
    temp_editor = "~2026-99999-01"

    scripted = [
        # Page 1: Unilever -> R1 (deleted), R4 (protection change)
        scripted_event(
            "e2e-unilever-delete",
            scripted_at,
            "Unilever",
            type="log",
            log_type="delete",
            log_action="delete",
            length=None,
        ),
        scripted_event(
            "e2e-unilever-protect",
            scripted_at + timedelta(seconds=20),
            "Unilever",
            type="log",
            log_type="protect",
            log_action="protect",
            length=None,
        ),
        # Page 2: Ben & Jerry's -> R2 high + R3 low (temporary account removes 75%)
        scripted_event(
            "e2e-benjerry-removal",
            scripted_at + timedelta(seconds=40),
            "Ben & Jerry's",
            user=temp_editor,
            length={"old": 12_000, "new": 3_000},
        ),
        # Page 3: Marmite -> R5: five edits inside one 10-minute window ...
        *[
            scripted_event(f"e2e-marmite-burst-{i}", r5_window + timedelta(minutes=i), "Marmite")
            for i in range(1, 6)
        ],
        # ... and only four in the next window: no second R5 alert
        *[
            scripted_event(
                f"e2e-marmite-quiet-{i}", r5_window + timedelta(minutes=10 + i), "Marmite"
            )
            for i in range(1, 5)
        ],
    ]
    malformed = scripted_event("e2e-malformed", scripted_at, "Nivea")
    del malformed["title"]

    phase1 = list(base)
    for i in range(DUPLICATE_EVERY - 1, len(base), DUPLICATE_EVERY):
        phase1.insert(min(len(phase1), i + 37), base[i])  # re-sent a little later
    phase1.extend(scripted)
    phase1.append(scripted[2])  # the R2/R3 edit delivered twice: still one alert each
    phase1.append(malformed)

    late_at = start - timedelta(minutes=10)
    late = []
    for i, original in enumerate(base[:LATE_EVENTS]):
        event = json.loads(json.dumps(original))
        event["meta"]["id"] = str(uuid.uuid5(NAMESPACE, f"late-{i}"))
        event["meta"]["dt"] = iso(late_at + timedelta(seconds=i))
        late.append(event)
    latest = max(end, r5_window + timedelta(minutes=15))
    flush = scripted_event(
        "e2e-watermark-flush",
        latest + timedelta(minutes=10),
        "Watermark flush page",
        user="Editor-flush",
    )

    expected_alerts = [
        {"rule_id": "R1", "severity": "high", "alert_id": sha("R1:e2e-unilever-delete")},
        {"rule_id": "R4", "severity": "low", "alert_id": sha("R4:e2e-unilever-protect")},
        {"rule_id": "R2", "severity": "high", "alert_id": sha("R2:e2e-benjerry-removal")},
        {"rule_id": "R3", "severity": "low", "alert_id": sha("R3:e2e-benjerry-removal")},
        {
            "rule_id": "R5",
            "severity": "medium",
            "alert_id": sha(f"R5:enwiki:Marmite:{r5_window.strftime('%Y-%m-%dT%H:%M:%S')}"),
        },
    ]
    unique_valid = len({e["meta"]["id"] for e in phase1 + late + [flush]} - {"e2e-malformed"})
    expected = {
        "phase1_lines": len(phase1),
        "phase2_lines": len(late) + 1,
        "unique_valid_events": unique_valid,
        "dlq_rows": 1,
        "alerts": expected_alerts,
        "late_meta_ids": [e["meta"]["id"] for e in late],
        "flush_meta_id": flush["meta"]["id"],
    }

    OUT.mkdir(parents=True, exist_ok=True)
    for name, events in (("e2e_phase1", phase1), ("e2e_phase2", late + [flush])):
        lines = "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events)
        if any(contains_ip(line) for line in lines.splitlines()):
            raise SystemExit(f"{name}: an IP-like string survived filtering")
        with gzip.open(OUT / f"{name}.jsonl.gz", "wt", encoding="utf-8") as f:
            f.write(lines)
    (OUT / "e2e_expected.json").write_text(json.dumps(expected, indent=2) + "\n")
    print(json.dumps({k: v for k, v in expected.items() if not k.endswith("ids")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
