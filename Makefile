# WikiWatch developer commands. See CLAUDE.md for the rules behind them.

ENV_FILE ?= .env.local
# On EC2 the boot script writes .image-tag.mk (IMAGE_TAG := <release tag>) after pulling
# the release's prebuilt images; without it, images are built locally as usual.
-include .image-tag.mk
# On EC2 (ENV_FILE=.env.aws) the cloud overlay swaps SeaweedFS and the REST catalog for S3 + Glue.
COMPOSE_FILES := -f docker-compose.yml $(if $(filter .env.aws,$(ENV_FILE)),-f docker-compose.aws.yml) \
	$(if $(IMAGE_TAG),-f docker-compose.prebuilt.yml)
COMPOSE  := $(if $(IMAGE_TAG),IMAGE_TAG=$(IMAGE_TAG) )docker compose $(COMPOSE_FILES) --env-file $(ENV_FILE)
VENV     := .venv
DBT      := DBT_PROFILES_DIR=dbt $(VENV)/bin/dbt --no-use-colors
DBT_ARGS := --project-dir dbt
PY       := $(VENV)/bin/python
STAMP    := $(VENV)/.installed
ALL_PROFILES := --profile core --profile airflow --profile dbt --profile producer
TF_STACKS := bootstrap foundation compute
TFLINT ?= tflint
# Node 24 LTS: Homebrew's keg-only node@24 if present, else whatever node is on PATH.
NODE24_BIN ?= /opt/homebrew/opt/node@24/bin
NODE_PATH := $(if $(wildcard $(NODE24_BIN)/node),PATH="$(NODE24_BIN):$$PATH")

.PHONY: help venv env-local up up-airflow up-dbt down smoke produce produce-stop producer-logs spark-logs replay load-ref maintain-lake alert-scenario check-lake test test-dags boot-check dbt-build dbt-docs lint tf-validate secrets-check e2e web web-s3 web-check check-env

help:
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | sort

venv: $(STAMP) ## Create .venv with Python 3.13 and the dev tools

$(STAMP): requirements-dev.txt producer/requirements.txt producer/pyproject.toml
	python3.13 -m venv $(VENV)
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -r requirements-dev.txt
	$(PY) -m pip install --quiet --no-deps -e producer
	touch $(STAMP)

env-local: ## Create .env.local with random local secrets (never overwrites)
	python3 scripts/init_env_local.py

check-env:
	@test -f $(ENV_FILE) || { echo "$(ENV_FILE) missing: run 'make env-local' first"; exit 1; }

up: check-env ## Start the core profile (Redpanda, SeaweedFS, Iceberg REST, Spark)
	$(COMPOSE) --profile core up -d --build --wait

up-airflow: check-env ## Start core plus Airflow
	$(COMPOSE) --profile core --profile airflow up -d --build --wait

up-dbt: check-env ## Start core plus Trino
	$(COMPOSE) --profile core --profile dbt up -d --build --wait

down: check-env ## Stop every profile (data volumes are kept)
	$(COMPOSE) $(ALL_PROFILES) down

smoke: check-env $(STAMP) ## Write an Iceberg table with Spark, read it with Trino (needs make up-dbt)
	@# Runs in a one-off container so it never competes with the streaming app for memory.
	$(COMPOSE) --profile core run --rm --no-deps -T --entrypoint /opt/spark/bin/spark-submit spark --driver-memory=768m /opt/wikiwatch/scripts/smoke_spark.py
	$(PY) scripts/smoke_trino.py

produce: check-env ## Start the producer: make produce MODE=fresh (first run) or MODE=resume
	@case "$(MODE)" in fresh|resume) ;; *) echo "MODE must be fresh or resume"; exit 1;; esac
	PRODUCER_MODE=$(MODE) $(COMPOSE) --profile core --profile producer up -d --build producer

produce-stop: check-env ## Stop the producer gracefully (final checkpoint runs)
	$(COMPOSE) --profile core --profile producer stop producer

producer-logs: check-env ## Follow the producer's JSON logs
	$(COMPOSE) --profile core --profile producer logs -f --no-log-prefix producer

spark-logs: check-env ## Follow the streaming app's logs
	$(COMPOSE) --profile core logs -f --no-log-prefix spark

replay: check-env $(STAMP) ## Publish a recorded fixture through the producer path: make replay FILE=...
	@test -n "$(FILE)" || { echo "usage: make replay FILE=path/to/events.jsonl"; exit 1; }
	$(PY) scripts/replay_fixture.py --file $(FILE)

load-ref: check-env ## Reload ref tables from dbt/seeds/*.csv (applies from the next micro-batch)
	$(COMPOSE) --profile core run --rm --no-deps -T --entrypoint /opt/spark/bin/spark-submit spark --driver-memory=768m /opt/wikiwatch/streaming/jobs/load_ref.py

maintain-lake: check-env ## Compact closed partitions and expire old snapshots of the stream's tables (start of a session)
	$(COMPOSE) --profile core run --rm --no-deps -T --entrypoint /opt/spark/bin/spark-submit spark --driver-memory=1g /opt/wikiwatch/streaming/jobs/maintain.py

alert-scenario: check-env $(STAMP) ## Replay scripted edits; check exact alerts and detection latency (needs make up-dbt)
	$(PY) scripts/run_alert_scenario.py

check-lake: check-env $(STAMP) ## Trino checks: Silver duplicates, Bronze offset gaps (needs make up-dbt)
	$(PY) scripts/check_lake.py

test: $(STAMP) ## Unit and contract tests, plus dbt unit tests when Trino is up
	$(PY) -m pytest
	@if curl -fs http://localhost:8085/v1/info >/dev/null 2>&1; then \
		$(DBT) test $(DBT_ARGS) --select "test_type:unit"; \
	else \
		echo ""; echo "WARNING: dbt unit tests SKIPPED - Trino is not running (start it with: make up-dbt)"; \
	fi

AIRFLOW_IMAGE := wikiwatch/airflow:3.3.2-dbt1.12.5

boot-check: check-env ## After make up-airflow: DAGs unpaused, one run of each DAG succeeds
	$(COMPOSE) --profile core --profile airflow exec -T airflow \
		python /opt/wikiwatch/airflow/tests/boot_check.py

test-dags: ## Import-check the Airflow DAGs inside the real Airflow image (no services needed)
	docker build -q -t $(AIRFLOW_IMAGE) docker/airflow >/dev/null
	docker run --rm -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db \
		-e AIRFLOW__CORE__LOAD_EXAMPLES=False \
		-v "$(CURDIR)/airflow/dags:/opt/airflow/dags:ro" \
		-v "$(CURDIR)/airflow/tests:/opt/wikiwatch/airflow/tests:ro" \
		-v "$(CURDIR)/orchestration:/opt/wikiwatch/orchestration:ro" \
		--entrypoint python $(AIRFLOW_IMAGE) /opt/wikiwatch/airflow/tests/check_dag_imports.py

dbt-build: check-env $(STAMP) ## dbt models and all dbt tests on local Trino (needs make up-dbt)
	$(DBT) build $(DBT_ARGS) --target local

dbt-docs: check-env $(STAMP) ## Generate dbt docs (lineage) into dbt/target/
	$(DBT) docs generate $(DBT_ARGS) --target local

lint: $(STAMP) ## ruff, sqlfluff, terraform fmt, tflint
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .
	@if find dbt -name '*.sql' | grep -q .; then $(VENV)/bin/sqlfluff lint dbt; else echo "sqlfluff: no SQL yet"; fi
	terraform fmt -check -recursive infra
	@command -v $(TFLINT) >/dev/null || { echo "tflint not found (install: docs/first-apply-checklist.md)"; exit 1; }
	$(TFLINT) --chdir=infra --init --config=$(CURDIR)/infra/.tflint.hcl >/dev/null
	$(TFLINT) --chdir=infra --recursive --config=$(CURDIR)/infra/.tflint.hcl

tf-validate: ## terraform init (no backend, no AWS) and validate for every stack
	@for stack in $(TF_STACKS); do \
		echo "== $$stack"; \
		terraform -chdir=infra/$$stack init -backend=false -input=false >/dev/null && \
		terraform -chdir=infra/$$stack validate -no-color || exit 1; \
	done

secrets-check: ## gitleaks on the full git history and the working tree
	gitleaks git --no-banner --redact .
	gitleaks dir --no-banner --redact .

e2e: $(STAMP) ## End-to-end replay test on a throwaway stack (dev stack must be down)
	$(PY) tests/e2e/run_e2e.py

web: web/node_modules ## Run the Next.js app on fixture snapshots (http://localhost:3000)
	cd web && $(NODE_PATH) npm run dev

web-s3: check-env web/node_modules ## Run the Next.js app on the snapshots in local SeaweedFS (needs make up)
	@cd web && $(NODE_PATH) SNAPSHOT_SOURCE=s3 S3_ENDPOINT=http://localhost:8333 \
		SNAPSHOT_BUCKET="$$(sed -n 's/^WAREHOUSE_BUCKET=//p' ../$(ENV_FILE))" \
		AWS_ACCESS_KEY_ID="$$(sed -n 's/^S3_ACCESS_KEY=//p' ../$(ENV_FILE))" \
		AWS_SECRET_ACCESS_KEY="$$(sed -n 's/^S3_SECRET_KEY=//p' ../$(ENV_FILE))" \
		npm run dev

web-check: web/node_modules ## Web type check, ESLint, tests and production build
	cd web && $(NODE_PATH) npm run typecheck && $(NODE_PATH) npm run lint && $(NODE_PATH) npm test && $(NODE_PATH) npm run build

web/node_modules: web/package-lock.json
	cd web && $(NODE_PATH) npm ci --no-audit --no-fund
	touch web/node_modules
