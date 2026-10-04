"""Reload ref.watchlist and ref.alert_rules from dbt/seeds/ (make load-ref).

Run after editing a seed CSV. The streaming app picks up the change in its next
micro-batch without a restart.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from streaming.lib.ref import load_ref
from streaming.lib.session import build_spark
from streaming.lib.tables import REF_ALERT_RULES, REF_WATCHLIST, create_tables


def main() -> None:
    spark = build_spark("wikiwatch-load-ref")
    create_tables(spark, (REF_WATCHLIST, REF_ALERT_RULES))
    counts = load_ref(spark, Path(os.environ.get("SEEDS_DIR", "/opt/wikiwatch/dbt/seeds")))
    print(json.dumps({"msg": "ref_loaded", **counts}))
    spark.stop()


if __name__ == "__main__":
    main()
