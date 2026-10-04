"""Query engines: Trino locally, Athena in the cloud, one interface.

`run(sql)` returns a list of dicts with Python values (datetime, date, int, float, bool,
str or None), whichever engine ran it. Only the engine changes between laptop and cloud;
the SQL is the same (ADR 0008).
"""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol


class Engine(Protocol):
    def run(self, sql: str) -> list[dict[str, Any]]: ...


def _plain(value: Any) -> Any:
    return float(value) if isinstance(value, Decimal) else value


class TrinoEngine:
    """Local Trino over its HTTP API (no credentials locally)."""

    def __init__(self, host: str, port: int, catalog: str = "iceberg") -> None:
        import trino

        self._conn = trino.dbapi.connect(
            host=host, port=port, user="orchestration", catalog=catalog
        )

    def run(self, sql: str) -> list[dict[str, Any]]:
        cur = self._conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        names = [d[0] for d in cur.description]
        return [{n: _plain(v) for n, v in zip(names, row, strict=True)} for row in rows]


class AthenaEngine:
    """Athena through boto3, in the cost-capped workgroup (1 GB per query).

    Credentials come from the instance role. Results are typed from Athena's column
    metadata, so callers see the same Python types as with Trino.
    """

    def __init__(self, workgroup: str, region: str, client=None, poll_seconds: float = 1.0):
        import boto3

        self._client = client or boto3.client("athena", region_name=region)
        self.workgroup = workgroup
        self.poll_seconds = poll_seconds

    def run(self, sql: str) -> list[dict[str, Any]]:
        query_id = self._client.start_query_execution(
            QueryString=sql,
            WorkGroup=self.workgroup,
            QueryExecutionContext={"Catalog": "AwsDataCatalog"},
        )["QueryExecutionId"]
        while True:
            status = self._client.get_query_execution(QueryExecutionId=query_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                reason = status["QueryExecution"]["Status"].get("StateChangeReason", state)
                raise RuntimeError(f"Athena query {state.lower()}: {reason}")
            time.sleep(self.poll_seconds)
        return list(self._results(query_id))

    def _results(self, query_id: str):
        paginator = self._client.get_paginator("get_query_results")
        first = True
        columns = []
        for page in paginator.paginate(QueryExecutionId=query_id):
            rs = page["ResultSet"]
            if first:
                columns = [(c["Name"], c["Type"]) for c in rs["ResultSetMetadata"]["ColumnInfo"]]
            rows = rs["Rows"][1:] if first else rs["Rows"]  # first row repeats the header
            first = False
            for row in rows:
                values = [d.get("VarCharValue") for d in row["Data"]]
                yield {n: athena_value(v, t) for (n, t), v in zip(columns, values, strict=True)}


def athena_value(text: str | None, athena_type: str) -> Any:
    """Convert one Athena result string to a Python value by its column type."""
    if text is None:
        return None
    kind = athena_type.lower()
    if kind in ("bigint", "integer", "int", "smallint", "tinyint"):
        return int(text)
    if kind in ("double", "float", "real") or kind.startswith("decimal"):
        return float(text)
    if kind == "boolean":
        return text == "true"
    if kind.startswith("timestamp"):
        return datetime.fromisoformat(text.replace(" UTC", ""))
    if kind == "date":
        return date.fromisoformat(text)
    return text


def engine_from_env(env: Mapping[str, str] | None = None) -> Engine:
    """QUERY_ENGINE=trino (local, default) or athena (cloud)."""
    env = os.environ if env is None else env
    kind = env.get("QUERY_ENGINE", "trino")
    if kind == "trino":
        return TrinoEngine(env.get("TRINO_HOST", "localhost"), int(env.get("TRINO_PORT", "8085")))
    if kind == "athena":
        return AthenaEngine(env["ATHENA_WORKGROUP"], env.get("AWS_REGION", "us-east-1"))
    raise ValueError(f"QUERY_ENGINE must be trino or athena, got {kind!r}")
