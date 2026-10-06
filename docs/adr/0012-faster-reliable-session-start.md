# ADR 0012: Faster, more reliable session start

Status: accepted (2026-10-06)

## Context

The first cloud sessions showed two costs at session start. Building the Spark, Airflow
and producer images on the instance took about 4 of the 5 boot minutes, and could produce
images that differ from the ones CI tested. And one spot request failed for lack of
t4g.xlarge capacity in the default zone, which needed a manual retry with other settings.

## Decisions

1. **Prebuilt images per release.** A new `images` workflow runs when a `v*` tag is
   pushed. It builds the three images natively on GitHub's ARM runners (free for public
   repositories; the instance is ARM) and publishes them to GitHub's container registry
   as `ghcr.io/advaithvemurisai/wikiwatch-<name>:<tag>`.
2. **Use them only when all three are there.** At boot the instance pulls the three
   images for its tag. If all succeed it writes `.image-tag.mk` (`IMAGE_TAG := <tag>`),
   and the Makefile adds `docker-compose.prebuilt.yml`, which swaps the local builds for
   the registry images. If any pull fails (not published yet, or still private), it builds
   locally exactly as before. A missing image can slow a session down, never stop it.
3. **Capacity fallback in demo-up.** `scripts/apply_session.py` retries an apply that
   fails with `InsufficientInstanceCapacity`: first in another zone that offers the type,
   then as on-demand. Any other error stops at once. Each attempt is time-limited
   (6-minute create timeout, 12-minute apply, 45-minute job), so Terraform always saves
   its state and releases the lock, and the job summary says what launched.

## Consequences

- Publishing images is a new moving part. The packages start private: make each one
  public once (docs/first-apply-checklist.md), or the instance keeps building locally.
- A release takes about 10 to 20 minutes to publish its images. A session started before
  that simply builds locally.
- The instance runs the images built from the release tag, the same Dockerfiles CI uses.
- A fallback to on-demand costs about $0.13 an hour instead of about $0.05 on spot, which
  is still cents per session, inside the budget guardrails of ADR 0010.
