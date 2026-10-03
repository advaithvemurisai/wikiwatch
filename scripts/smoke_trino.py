"""Smoke test, part 2 (runs on the host via `make smoke`).

Reads the table written by smoke_spark.py through Trino, checks the row count and the
Iceberg format version, then drops the table and schema so the lake stays clean.
"""

from __future__ import annotations

import os
import re
import sys

import trino

EXPECTED_ROWS = 3
TABLE = "iceberg.smoke.t"


def main() -> int:
    """Return 0 if Trino sees exactly the rows Spark wrote, 1 otherwise."""
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8085")),
        user="smoke",
        catalog="iceberg",
    )
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT count(*) FROM {TABLE}")
        count = cur.fetchone()[0]
        cur.execute(f"SHOW CREATE TABLE {TABLE}")
        ddl = cur.fetchone()[0]
        format_version = re.search(r"format_version\s*=\s*(\d+)", ddl)
        version = format_version.group(1) if format_version else "unknown"
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.fetchall()
        cur.execute("DROP SCHEMA IF EXISTS iceberg.smoke")
        cur.fetchall()

    if count != EXPECTED_ROWS or version != "2":
        print(f"Trino: FAILED (rows={count}, format_version={version})")
        return 1
    print(f"Trino: read {count} rows from {TABLE}, format_version={version}. Smoke test passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
