# WikiWatch: real-time brand page monitoring on a streaming lakehouse

Communications teams find out about risky Wikipedia edits hours late. WikiWatch streams
every Wikimedia edit, checks it against a watchlist of brand pages, and raises alerts
within minutes, with no lost or duplicated edits across restarts.

Redpanda (Kafka API) → Spark Structured Streaming → Apache Iceberg on S3 → dbt on Athena
→ Next.js on Vercel. Deployed with Terraform, tested end to end in CI.

> Work in progress: the local stack is in place (Task 1 of [TASKS.md](TASKS.md)).
> Measured results will be added after the first AWS session.

## Run locally

Requirements: Docker Desktop (memory set to 6 GB), Python 3.13, make.

```bash
make env-local   # create .env.local with random throwaway local secrets
make up-dbt      # start Redpanda, SeaweedFS, the Iceberg REST catalog, Spark and Trino
make smoke       # write an Iceberg table with Spark and read it back with Trino
```

Run `make venv` once beforehand to install the Python dev tools, and `make down` to stop.
On an 8 GB machine, run `core` plus at most one extra profile: `make up`, `make up-dbt` or
`make up-airflow`.

| UI | Address |
| --- | --- |
| Redpanda Console | http://localhost:8088 |
| Airflow (with `make up-airflow`) | http://localhost:8080 |
| Trino (with `make up-dbt`) | http://localhost:8085 |
| Spark UI (while a job runs) | http://localhost:4040 |

## Design documents

- [docs/v1.md](docs/v1.md): v1 scope, business story, alert rules, data model
- [docs/plan.md](docs/plan.md): full technical design
- [docs/adr/](docs/adr/): architecture decision records
