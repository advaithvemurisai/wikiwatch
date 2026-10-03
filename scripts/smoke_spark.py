"""Smoke test, part 1 (runs inside the Spark container via `make smoke`).

Creates lake.smoke.t as an Iceberg format-version 2 table partitioned by UTC day and
writes 3 rows through the catalog chosen by the env file. Part 2 (smoke_trino.py) reads
the rows back through Trino.
"""

from __future__ import annotations

from streaming.lib.session import build_spark

EXPECTED_ROWS = 3


def main() -> None:
    """Create the smoke table and write the test rows."""
    spark = build_spark("wikiwatch-smoke")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.smoke")
    spark.sql("DROP TABLE IF EXISTS lake.smoke.t PURGE")
    spark.sql(
        """
        CREATE TABLE lake.smoke.t (id INT, note STRING, ts TIMESTAMP)
        USING iceberg
        PARTITIONED BY (days(ts))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(
        """
        INSERT INTO lake.smoke.t VALUES
          (1, 'first',  TIMESTAMP '2026-10-01 00:00:00'),
          (2, 'second', TIMESTAMP '2026-10-02 12:30:00'),
          (3, 'third',  TIMESTAMP '2026-10-03 23:59:59')
        """
    )
    count = spark.table("lake.smoke.t").count()
    if count != EXPECTED_ROWS:
        raise SystemExit(f"Spark wrote {count} rows, expected {EXPECTED_ROWS}")
    print(f"Spark: wrote {count} rows to lake.smoke.t (Iceberg format-version 2)")
    spark.stop()


if __name__ == "__main__":
    main()
