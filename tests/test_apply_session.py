"""scripts/apply_session.py: which (market, zone) demo-up tries after a capacity shortage."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("apply_session", ROOT / "scripts/apply_session.py")
apply_session = importlib.util.module_from_spec(spec)
spec.loader.exec_module(apply_session)
attempts = apply_session.attempts

ZONES = ["us-east-1c", "us-east-1a", "us-east-1b"]


def test_spot_tries_another_zone_then_on_demand():
    assert attempts("spot", "", ZONES) == [
        ("spot", ""),  # the default zone, which is the first offered: us-east-1a
        ("spot", "us-east-1b"),
        ("on-demand", ""),
    ]


def test_a_chosen_zone_is_not_retried_as_the_other_zone():
    assert attempts("spot", "us-east-1b", ZONES)[1] == ("spot", "us-east-1a")


def test_on_demand_only_moves_zones():
    assert attempts("on-demand", "", ZONES) == [("on-demand", ""), ("on-demand", "us-east-1b")]


def test_a_single_offered_zone_still_falls_back_to_on_demand():
    assert attempts("spot", "", ["us-east-1a"]) == [("spot", ""), ("on-demand", "")]


def fake_terraform(results):
    """Terraform stand-in: apply results in order ("ok", "capacity" or "other")."""
    calls = []

    def run(args, log, env, timeout=None):
        if args[0] == "plan":
            log.write_text("planned\n")
            return 0
        outcome = results[len(calls)]
        calls.append((env["TF_VAR_market"], env["TF_VAR_availability_zone"]))
        log.write_text({"ok": "Apply complete!\n",
                        "capacity": "Error: InsufficientInstanceCapacity: none\n\n",
                        "other": "Error: UnauthorizedOperation: denied\n\n"}[outcome])  # fmt: skip
        return 0 if outcome == "ok" else 1

    return run, calls


def run_main(monkeypatch, tmp_path, results, market="spot"):
    run, calls = fake_terraform(results)
    monkeypatch.setattr(apply_session, "terraform", run)
    monkeypatch.setattr(apply_session, "offered_zones", lambda region: ZONES)
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setenv("TF_VAR_market", market)
    monkeypatch.setenv("TF_VAR_availability_zone", "")
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    monkeypatch.chdir(tmp_path)
    return apply_session.main(), calls


def test_capacity_shortages_move_on_until_one_launches(monkeypatch, tmp_path):
    code, calls = run_main(monkeypatch, tmp_path, ["capacity", "capacity", "ok"])
    assert code == 0
    assert calls == [("spot", ""), ("spot", "us-east-1b"), ("on-demand", "")]


def test_any_other_error_stops_at_once(monkeypatch, tmp_path):
    code, calls = run_main(monkeypatch, tmp_path, ["other", "ok"])
    assert code == 1
    assert calls == [("spot", "")]


def test_gives_up_after_the_last_attempt(monkeypatch, tmp_path):
    code, calls = run_main(monkeypatch, tmp_path, ["capacity"] * 3)
    assert code == 1 and len(calls) == 3
    assert not (tmp_path / "apply.log").exists(), "Terraform's log never outlives the step"


def test_terraform_flags_come_before_the_plan_file(monkeypatch, tmp_path):
    seen = []

    def run(cmd, **_):
        seen.append(cmd)
        return type("Done", (), {"returncode": 0})()

    monkeypatch.setattr(apply_session.subprocess, "run", run)
    apply_session.terraform(["apply", "tfplan"], tmp_path / "log", {}, timeout="12m")
    assert seen[0][-4:] == ["apply", "-input=false", "-no-color", "tfplan"]
