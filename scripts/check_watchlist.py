"""Check every watchlist title against the live Wikipedia API.

A title that is a redirect or misspelled never matches an edit, so its page would be
silently unmonitored. Also enforces "no biographies of living people" (docs/v1.md).

Usage: WIKIWATCH_USER_AGENT="WikiWatch/0.1 (<contact>)" python scripts/check_watchlist.py
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
API = "https://en.wikipedia.org/w/api.php"


def check(titles: list[str], user_agent: str) -> dict[str, list[str]]:
    """Return {title: [problems]} for titles that are missing, redirects, etc."""
    problems: dict[str, list[str]] = {}
    for i in range(0, len(titles), 50):
        chunk = titles[i : i + 50]
        response = httpx.get(
            API,
            params={
                "action": "query",
                "format": "json",
                "formatversion": "2",
                "titles": "|".join(chunk),
                "redirects": "1",
                "prop": "categories|pageprops",
                "clcategories": "Category:Living people",
                "ppprop": "disambiguation",
            },
            headers={"User-Agent": user_agent},
            timeout=30,
        )
        response.raise_for_status()
        query = response.json()["query"]
        for redirect in query.get("redirects", []):
            problems.setdefault(redirect["from"], []).append(f"redirects to {redirect['to']!r}")
        for normalized in query.get("normalized", []):
            problems.setdefault(normalized["from"], []).append(
                f"not the canonical title ({normalized['to']!r})"
            )
        for page in query["pages"]:
            title = page["title"]
            if page.get("missing"):
                problems.setdefault(title, []).append("page does not exist")
            if page.get("categories"):
                problems.setdefault(title, []).append("biography of a living person")
            if "disambiguation" in page.get("pageprops", {}):
                problems.setdefault(title, []).append("disambiguation page")
            if page.get("ns", 0) != 0:
                problems.setdefault(title, []).append("not an article")
    return problems


def main() -> int:
    user_agent = os.environ.get("WIKIWATCH_USER_AGENT", "")
    if "(" not in user_agent:
        print("Set WIKIWATCH_USER_AGENT to 'WikiWatch/0.1 (<contact>)' first.")
        return 2
    with (ROOT / "dbt/seeds/watchlist.csv").open() as f:
        titles = [row["title"] for row in csv.DictReader(f) if row["wiki"] == "enwiki"]
    problems = check(titles, user_agent)
    for title, issues in sorted(problems.items()):
        print(f"{title}: {'; '.join(issues)}")
    print(f"checked {len(titles)} titles, {len(problems)} with problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
