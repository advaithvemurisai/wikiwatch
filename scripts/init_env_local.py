"""Create .env.local from .env.example with random throwaway secrets, or add new keys.

Every value equal to "changeme" is replaced with a fresh random secret. The secrets are
written to the file only; they are never printed. An existing .env.local is never
overwritten: keys that .env.example gained since it was created are appended (with fresh
secrets where needed), and only their names are printed. Re-running is always safe.

Usage: python scripts/init_env_local.py [--example PATH] [--target PATH]
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
from pathlib import Path

PLACEHOLDER = "changeme"
REPO_ROOT = Path(__file__).resolve().parent.parent


def random_value(key: str) -> str:
    """Return a random secret suitable for the given variable name."""
    if key == "AIRFLOW_FERNET_KEY":
        # Fernet keys must be 32 url-safe base64-encoded bytes.
        return base64.urlsafe_b64encode(os.urandom(32)).decode()
    return secrets.token_urlsafe(24)


def render(example_text: str) -> str:
    """Return the env file text with every placeholder value replaced."""
    lines = []
    for line in example_text.splitlines():
        key, sep, value = line.partition("=")
        if sep and not line.lstrip().startswith("#") and value.strip() == PLACEHOLDER:
            line = f"{key}={random_value(key.strip())}"
        lines.append(line)
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--example", type=Path, default=REPO_ROOT / ".env.example")
    parser.add_argument("--target", type=Path, default=REPO_ROOT / ".env.local")
    args = parser.parse_args(argv)

    rendered = render(args.example.read_text())
    if not args.target.exists():
        args.target.write_text(rendered)
        args.target.chmod(0o600)
        print(f"Created {args.target.name} with random local secrets.")
        return 0
    existing = env_keys(args.target.read_text())
    missing = [
        line for line in rendered.splitlines() if (key := env_key(line)) and key not in existing
    ]
    if not missing:
        print(f"{args.target.name} is up to date; leaving it unchanged.")
        return 0
    with args.target.open("a") as f:
        f.write("\n# Added by make env-local\n" + "\n".join(missing) + "\n")
    print(f"Added to {args.target.name}: {', '.join(env_key(line) for line in missing)}")
    return 0


def env_key(line: str) -> str | None:
    """Return the variable name of an assignment line, or None for comments and blanks."""
    key, sep, _ = line.partition("=")
    if not sep or line.lstrip().startswith("#"):
        return None
    return key.strip()


def env_keys(text: str) -> set[str]:
    return {key for line in text.splitlines() if (key := env_key(line))}


if __name__ == "__main__":
    sys.exit(main())
