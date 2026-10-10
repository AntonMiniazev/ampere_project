FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DBT_PROJECT_DIR=/app/dbt_iceberg \
    DBT_PROFILES_DIR=/app/profiles \
    DUCKDB_SECRET_DIRECTORY=/app/secret_store

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY docker/dbt_iceberg/requirements.txt /tmp/requirements.txt
RUN python -m pip install -r /tmp/requirements.txt \
    && dbc install --level system 'duckdb=1.5.6' \
    && python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL iceberg'); c.execute('INSTALL httpfs')"

COPY dbt_iceberg /app/dbt_iceberg
COPY tools/__init__.py /app/tools/__init__.py
COPY tools/contracts/ /app/tools/contracts/
COPY tools/budget_generation/budget_parameters_daily.csv /app/budget_parameters_daily.csv
COPY docker/dbt_iceberg/prepare_catalog.py /app/prepare_catalog.py
COPY docker/dbt_iceberg/duckdb_runtime.py /app/duckdb_runtime.py
COPY docker/dbt_iceberg/publish_catalog.py /app/publish_catalog.py
COPY docker/dbt_iceberg/entrypoint.sh /usr/local/bin/ampere_dbt_iceberg_entrypoint.sh
RUN chmod +x /usr/local/bin/ampere_dbt_iceberg_entrypoint.sh \
    && mkdir -p /app/artifacts /app/profiles /app/secret_store

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/ampere_dbt_iceberg_entrypoint.sh"]
