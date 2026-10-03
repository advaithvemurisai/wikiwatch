# ADR 0002: SeaweedFS instead of MinIO for local S3

Status: accepted (2026-10-03)

## Context

The original design used MinIO as the local stand-in for S3. MinIO's community edition
is no longer maintained: its repository was put into maintenance mode in December 2025
and archived in 2026, and the `minio/minio` Docker Hub repository was deleted in
September 2026. There are no security fixes and no official images to pin.

Candidates checked on 2026-10-03:

| Option | License | Notes |
| --- | --- | --- |
| SeaweedFS 4.48 | Apache-2.0 | Maintained since 2012, multi-arch images, single-container S3 mode |
| S3Proxy 4.1.1 | Apache-2.0 | Very light, single maintainer |
| RustFS 1.0.1 | Apache-2.0 | Closest to MinIO's UX, young project with a recent serious vulnerability |
| Garage | AGPL-3.0 | Built for distributed clusters; more setup than a local demo needs |

## Decision

Use SeaweedFS in single-container S3 mode for the local `core` profile, pinned to an
exact release in Task 1. The cloud side is unchanged: Amazon S3.

## Consequences

- Spark (Iceberg `S3FileIO`), Trino and the producer reach it through the S3 API with
  path-style access, so the code only changes endpoint and credentials by environment file.
- SeaweedFS is not a perfect S3 clone. Anything the pipeline depends on (multipart upload,
  conditional writes, listing) is exercised by the Task 1 smoke test and the e2e test, and
  any gap gets written down here.
- The governance risk remains: no open-source object store is foundation-backed, so
  licences can change. Keeping all access behind the S3 API keeps a future swap cheap.

Sources: https://linuxiac.com/minio-ends-active-development/,
https://rmoff.net/2026/01/14/alternatives-to-minio-for-single-node-local-s3/
