"""demo-up's apply, with a fallback for capacity shortages.

A session needs one t4g.xlarge. When AWS has none to spare (InsufficientInstanceCapacity),
this retries in another zone that offers the type, then as on-demand, instead of failing
the run. Any other error stops at once. Each attempt is plan + apply under a timeout that
ends inside the job's limit, so Terraform always saves its state and releases the lock.

Terraform's output goes to a file, never to the log (it holds ARNs); a failure prints only
the masked Error: lines (scripts/tf_errors.py).

Usage (in infra/compute, after terraform init): python3 scripts/apply_session.py
Reads TF_VAR_market and TF_VAR_availability_zone set by the "Check the inputs" step.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

INSTANCE_TYPE = "t4g.xlarge"
CAPACITY_ERROR = "InsufficientInstanceCapacity"
SCRIPTS = Path(__file__).resolve().parent


def attempts(market: str, zone: str, offered: list[str]) -> list[tuple[str, str]]:
    """(market, zone) to try in order. "" means the default zone (first offered)."""
    zones = sorted(offered)
    first = zone or (zones[0] if zones else "")
    order = [(market, zone)]
    other = next((z for z in zones if z != first), None)
    if other:
        order.append((market, other))
    if market == "spot":
        order.append(("on-demand", ""))
    return order


def offered_zones(region: str) -> list[str]:
    out = subprocess.run(  # noqa: S603 - fixed arguments; the runner's own AWS CLI
        ["aws", "ec2", "describe-instance-type-offerings", "--region", region,  # noqa: S607
         "--location-type", "availability-zone",
         "--filters", f"Name=instance-type,Values={INSTANCE_TYPE}",
         "--query", "InstanceTypeOfferings[].Location", "--output", "text"],
        capture_output=True, text=True, check=True,
    )  # fmt: skip
    return out.stdout.split()


def terraform(args: list[str], log: Path, env: dict, timeout: str | None = None) -> int:
    cmd = ["terraform", *args, "-input=false", "-no-color"]
    if timeout:  # INT first: Terraform stops cleanly, saves state, releases the lock
        cmd = ["timeout", "--signal=INT", "--kill-after=3m", timeout, *cmd]
    with log.open("w") as handle:
        return subprocess.run(cmd, stdout=handle, stderr=subprocess.STDOUT, env=env).returncode  # noqa: S603


def main() -> int:
    temp = Path(os.environ.get("RUNNER_TEMP", "/tmp"))  # noqa: S108
    log, plan = temp / "apply.log", Path("tfplan")
    market = os.environ.get("TF_VAR_market", "spot")
    zone = os.environ.get("TF_VAR_availability_zone", "")
    tried = attempts(market, zone, offered_zones(os.environ.get("AWS_REGION", "us-east-1")))
    for number, (try_market, try_zone) in enumerate(tried, start=1):
        where = try_zone or "the default zone"
        print(f"Attempt {number} of {len(tried)}: {try_market} in {where}", flush=True)
        env = {**os.environ, "TF_VAR_market": try_market, "TF_VAR_availability_zone": try_zone}
        if terraform(["plan", f"-out={plan}"], log, env) == 0 and (
            terraform(["apply", str(plan)], log, env, timeout="12m") == 0
        ):
            log.unlink(missing_ok=True)
            plan.unlink(missing_ok=True)
            summary = os.environ.get("GITHUB_STEP_SUMMARY")
            if summary:
                with open(summary, "a") as handle:
                    handle.write(f"Session launched: {try_market}, {where}.\n")
            return 0
        text = log.read_text(errors="replace")
        subprocess.run(  # noqa: S603 - this repo's own script
            [sys.executable, str(SCRIPTS / "tf_errors.py"), str(log)], check=False
        )
        if CAPACITY_ERROR not in text:
            break  # a real error: retrying elsewhere would not help
        print(f"No {INSTANCE_TYPE} capacity for {try_market} in {where}.", flush=True)
    log.unlink(missing_ok=True)
    plan.unlink(missing_ok=True)
    print("Apply failed. Anything already created is in the state: run demo-down to remove it.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
