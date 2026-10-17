"""Checks on the dbt project's override of dbt-athena's resolve_table_type (Athena only).

dbt's unit tests first create an empty table from the model's SQL; without the override
dbt-athena makes it a Hive table, which cannot hold timestamp(6), so every burst_alerts
unit test errored on Athena. Trino never calls this macro, so local runs cannot catch a
regression; these tests render the macro directly and guard against drift from the
pinned dbt-athena's own version.
"""

from __future__ import annotations

import re
from pathlib import Path

import jinja2
import pytest

ROOT = Path(__file__).resolve().parent.parent
OURS = ROOT / "dbt" / "macros" / "resolve_table_type.sql"


class _Return(Exception):  # dbt implements return() the same way
    def __init__(self, value):
        self.value = value


def _raise_return(value):
    raise _Return(value)


class _Config:
    def __init__(self, **values):
        self.values = values
        self.model = object()

    def get(self, key, default=None):
        return self.values.get(key, default)


class _Adapter:
    def build_catalog_relation(self, model):
        return None


def resolve(**config) -> str:
    # Renders SQL, not HTML, so nothing to escape.
    env = jinja2.Environment(extensions=["jinja2.ext.do"], autoescape=False)  # noqa: S701
    module = env.from_string(OURS.read_text()).make_module(
        {"config": _Config(**config), "adapter": _Adapter(), "return": _raise_return}
    )
    with pytest.raises(_Return) as returned:
        module.resolve_table_type()
    return returned.value.value


def macro_body(text: str) -> list[str]:
    """The macro's statements, without comments, blank lines or indentation."""
    text = re.sub(r"\{#.*?#\}", "", text, flags=re.DOTALL)
    body = text.split("{% macro resolve_table_type() %}", 1)[1].split("{% endmacro %}", 1)[0]
    return [line.strip() for line in body.splitlines() if line.strip()]


def test_unit_tests_build_their_temp_table_as_iceberg():
    assert resolve(materialized="unit") == "iceberg"


@pytest.mark.parametrize(
    ("config", "expected"),
    [({"materialized": "incremental", "table_type": "iceberg"}, "iceberg"),
     ({"materialized": "table"}, "hive"),
     ({"materialized": "table", "table_type": "HIVE"}, "hive")],
)  # fmt: skip
def test_models_keep_dbt_athenas_behaviour(config, expected):
    assert resolve(**config) == expected


def test_override_matches_the_pinned_dbt_athena_macro():
    """After a dbt-athena upgrade, re-copy its macro body below the unit-test branch."""
    import dbt.include.athena as athena

    theirs = Path(athena.__file__).parent / "macros/materializations/models/helpers"
    theirs_body = macro_body((theirs / "resolve_table_type.sql").read_text())
    ours_body = macro_body(OURS.read_text())
    assert ours_body[:3] == [
        "{%- if config.get('materialized') == 'unit' -%}",
        "{{ return('iceberg') }}",
        "{%- endif -%}",
    ]
    assert ours_body[3:] == theirs_body
