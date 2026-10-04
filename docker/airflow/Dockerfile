# Airflow 3.3.2 for the dbt_gold and freshness_monitor DAGs (Task 8, ADR 0011).
FROM apache/airflow:3.3.2-python3.13

# The orchestration package's libraries go into Airflow's own environment, constrained to
# the exact versions already installed in the official image (pip freeze), so nothing
# Airflow ships with can be upgraded or downgraded. New packages are pinned to match the
# laptop and CI.
RUN pip freeze > /tmp/airflow-installed.txt \
    && pip install --no-cache-dir trino==0.340.0 confluent-kafka==2.15.1 jsonschema \
        --constraint /tmp/airflow-installed.txt \
    && rm /tmp/airflow-installed.txt

# dbt gets its own virtual environment: dbt and Airflow pin different versions of shared
# libraries (Jinja, protobuf, ...), and the DAG calls dbt as a command anyway.
# dbt-athena needs mmh3 < 4.2, which has no prebuilt wheel for Python 3.13 on ARM, so a C
# compiler is installed for this step only and removed again (no build tools ship).
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && su airflow -c "python -m venv /home/airflow/dbt-venv \
        && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
            dbt-core==1.12.5 dbt-trino==1.10.6 dbt-athena==1.11.1" \
    && apt-get purge -y gcc libc6-dev \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
USER airflow

ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt \
    PYTHONPATH=/opt/wikiwatch \
    DASHBOARD_SCHEMA_DIR=/opt/wikiwatch/schemas/dashboard
