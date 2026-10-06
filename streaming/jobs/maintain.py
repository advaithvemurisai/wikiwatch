"""One-off Iceberg maintenance: `make maintain-lake` (see streaming/lib/maintenance.py).

Run it at the start of a session, before `make produce`, while the stream is idle.
Compaction skips today's partitions, so running it during a session is safe too, but the
stream's commits may then retry once.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from streaming.lib.maintenance import run
from streaming.lib.session import build_spark


def main() -> None:
    spark = build_spark("wikiwatch-maintenance")
    now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    for summary in run(spark, now):
        print(json.dumps({"msg": "maintained", **summary}, default=str), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
