# init_source_preparation

Standalone batch application that bootstraps the Source (Pre-Raw) schema in PostgreSQL and loads initial dictionaries plus core seed data.

## Runtime configuration (env vars)

- `DATABASE_URL`: full SQLAlchemy/psycopg URL (recommended)
- `PGUSER`/`pguser` and `PGPASSWORD`/`pgpass`: credentials for PostgreSQL
- `PGHOST`, `PGPORT`, `PGDATABASE`: optional overrides (defaults live in `config.py`)
- `SCHEMA_INIT`: target schema (default: `source`)
- `N_OF_INIT_CLIENTS`: number of clients to generate (default: `20000`)
- `N_DELIVERY_RESOURCE`: number of delivery resources (default: `700`)
- `PROJECT_START_DATE`: YYYY-MM-DD base date (default: current date)

## Local run

```bash
cd docker/init_source_preparation
export DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/source"
python -m init_source_preparation
```

## Docker build/run

```bash
docker build -t init-source-preparation:latest .

docker run --rm \
  -e DATABASE_URL="postgresql+psycopg://user:pass@host:5432/source" \
  -e SCHEMA_INIT=core \
  init-source-preparation:latest
```

## Airflow

Launch this image via your Airflow DAG, passing the same environment variables.

## Image tagging (GitHub Actions)

Image tag is read from `docker/image_tags.json` using key `source_preparation`.

## Autovacuum and indexing notes

Indexes are created during init (see `ddl_init.py`). Keep autovacuum enabled
and tune it at the Postgres level; avoid reindexing after every generator run.
