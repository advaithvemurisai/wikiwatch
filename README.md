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

To stream live edits, add a User-Agent with your contact details to `.env.local`
(`WIKIWATCH_USER_AGENT=WikiWatch/0.1 (<your contact URL>)`, as Wikimedia requires), then run
`make produce MODE=fresh` the first time and `make produce MODE=resume` after any stop.
The producer is at-least-once: it saves its stream position only after Redpanda has
acknowledged everything before it, so a crash can repeat events but never skip one.

The Spark streaming app starts with the stack and waits until the producer has created the
topics. It writes Bronze (raw events) and Silver (deduplicated, typed edits) Iceberg tables;
`make check-lake` (with `make up-dbt`) verifies Silver has no duplicates and Bronze has no gaps.

The watchlist (`dbt/seeds/watchlist.csv`) is 60 real English Wikipedia company and product
pages, no biographies. Brightline Brands is fictional: Unilever and its brands stand in as its
"own brand" and products, and other consumer goods companies stand in as competitors. To
change a page or an alert threshold, edit the CSV and run `make load-ref`; the stream applies
it from the next micro-batch. `make alert-scenario` replays scripted edits and checks that
exactly the expected alerts appear.

Batch Gold models (R5 burst alerts, complete per-minute windows, hourly bot share and a
daily digest per watched page) are built with dbt: `make dbt-build` runs them and every dbt
test on local Trino, and the same project runs on Athena in the cloud.

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
