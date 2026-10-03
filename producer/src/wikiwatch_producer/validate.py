"""Validate raw SSE payloads against schemas/wiki_edits.json (invariant 4)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parents[3] / "schemas" / "wiki_edits.json"


@dataclass(frozen=True)
class Result:
    """Outcome of validating one payload: exactly one of event or reason is set."""

    event: dict[str, Any] | None
    reason: str | None

    @property
    def ok(self) -> bool:
        return self.event is not None


class EventValidator:
    """Parses and validates recentchange payloads.

    Rejection reasons name the failing field path and rule only (for example
    ``meta.id: required``), never the offending value, so an IP address or other
    personal data cannot leak into the DLQ reason or the logs.
    """

    def __init__(self, schema_path: Path = DEFAULT_SCHEMA_PATH) -> None:
        self.schema = json.loads(schema_path.read_text())
        Draft202012Validator.check_schema(self.schema)
        self._validator = Draft202012Validator(self.schema)

    def validate(self, raw: str) -> Result:
        """Return the parsed event, or the reason it was rejected."""
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return Result(None, "invalid_json")
        if not isinstance(event, dict):
            return Result(None, "not_an_object")
        error = next(iter(sorted(self._validator.iter_errors(event), key=_error_order)), None)
        if error is None:
            return Result(event, None)
        return Result(None, _reason(error))


def _error_order(error) -> tuple:
    return (len(error.absolute_path), list(map(str, error.absolute_path)), error.validator)


def _reason(error) -> str:
    path = ".".join(str(part) for part in error.absolute_path)
    if error.validator == "required":
        # The message names the missing property, which is schema text, not data.
        missing = error.message.split("'")[1] if "'" in error.message else "?"
        path = f"{path}.{missing}" if path else missing
    return f"{path or '<root>'}: {error.validator}"
