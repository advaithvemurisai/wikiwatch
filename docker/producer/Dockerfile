# WikiWatch producer: Wikimedia SSE stream -> Redpanda (Task 2).
FROM python:3.13.16-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    WIKIWATCH_SCHEMA_PATH=/app/schemas/wiki_edits.json

WORKDIR /app
COPY producer/requirements.txt producer/requirements.txt
RUN pip install -r producer/requirements.txt
COPY schemas/wiki_edits.json schemas/wiki_edits.json
COPY producer/pyproject.toml producer/pyproject.toml
COPY producer/src producer/src
RUN pip install --no-deps ./producer

USER nobody
ENTRYPOINT ["python", "-m", "wikiwatch_producer"]
