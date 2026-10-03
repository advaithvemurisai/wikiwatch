# WikiWatch developer commands. See CLAUDE.md for the rules behind them.

ENV_FILE ?= .env.local
COMPOSE  := docker compose --env-file $(ENV_FILE)
VENV     := .venv
PY       := $(VENV)/bin/python
ALL_PROFILES := --profile core --profile airflow --profile dbt

.PHONY: help venv env-local up up-airflow up-dbt down smoke test lint secrets-check e2e web check-env

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | sort

venv: ## Create .venv with Python 3.13 and the dev tools
	python3.13 -m venv $(VENV)
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -r requirements-dev.txt

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

smoke: check-env ## Write an Iceberg table with Spark, read it with Trino (needs make up-dbt)
	$(COMPOSE) exec -T spark sh -c '/opt/spark/bin/spark-submit --driver-memory "$$SPARK_DRIVER_MEMORY" /opt/wikiwatch/scripts/smoke_spark.py'
	$(PY) scripts/smoke_trino.py

test: ## Unit and contract tests (dbt tests arrive in Task 5)
	$(PY) -m pytest

lint: ## ruff, sqlfluff, terraform fmt, tflint
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .
	@if find dbt -name '*.sql' | grep -q .; then $(VENV)/bin/sqlfluff lint dbt; else echo "sqlfluff: no SQL yet"; fi
	terraform fmt -check -recursive infra
	@if command -v tflint >/dev/null; then tflint --chdir=infra --recursive; else echo "tflint: not installed (needed from Task 7)"; fi

secrets-check: ## gitleaks on the full git history and the working tree
	gitleaks git --no-banner --redact .
	gitleaks dir --no-banner --redact .

e2e: ## End-to-end fixture replay test
	@echo "make e2e is added in Task 6"; exit 1

web: ## Run the Next.js app on fixture snapshots
	@echo "make web is added in Task 8"; exit 1
