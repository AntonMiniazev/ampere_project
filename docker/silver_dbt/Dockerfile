FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/app \
    DBT_PROJECT_DIR=/app/dbt \
    DBT_PROFILES_DIR=/app/profiles \
    DUCKDB_PATH=/app/artifacts/ampere.duckdb \
    DBT_ARTIFACT_LOG_DIR=/app/logs \
    DBT_LOG_PATH=/app/logs \
    THREADS=4 \
    DBT_TARGET=prod

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /app/.venv

COPY docker/silver_dbt/requirements.txt /tmp/requirements.txt

RUN /app/.venv/bin/pip install --upgrade pip \
    && /app/.venv/bin/pip install -r /tmp/requirements.txt

COPY dbt /app/dbt
COPY docker/silver_dbt/scripts /app/scripts
COPY docker/silver_dbt/entrypoint.sh /usr/local/bin/silver_dbt_entrypoint.sh
COPY docker/silver_dbt/render_profiles.sh /usr/local/bin/render_profiles.sh

RUN chmod +x /usr/local/bin/silver_dbt_entrypoint.sh /usr/local/bin/render_profiles.sh \
    && mkdir -p /app/artifacts /app/profiles /app/logs

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/silver_dbt_entrypoint.sh"]
